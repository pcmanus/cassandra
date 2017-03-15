/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net.async;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Represents one connection to a peer, and handles the state transistions on the connection and the netty {@link Channel}
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * The basic setup for the channel is like this: a message is requested to be sent via {@link #sendMessage(MessageOut, int)}.
 * If the channel is not established, then we need to create it (obviously). To prevent multiple threads from creating
 * independent connections, they attempt to update the {@link #state} via the {@link #stateUpdater}; one thread will win the race
 * and create the connection. Upon sucessfully setting up the connection/channel, the {@link #state} will be updated again
 * (to {@link State#READY}, which indicates to other threads that the channel is ready for business and can be written to.
 *
 * Note on flushing: when sending a message to the netty {@link Channel}, we call {@link Channel#write(Object)} when
 * coalescing is enabled but {@link Channel#writeAndFlush(Object)} is disabled. The reason is that, at least as of netty
 * 4.1.8, {@link Channel#writeAndFlush(Object)} causes the  netty event loop thread to be woken up, whereas
 * {@link Channel#write(Object)} does not. And without coalescing, we want the thread to wake up so the write is
 * processed immediately or otherwise processing latency would be introduced (this doesn't trigger a genuine flush
 * however due to our overriding in {@link MessageOutHandler#flush(ChannelHandlerContext)}). With coalescing however,
 * there is no point in waking up the thread until the conditions for the coalesce have been fulfilled (number of
 * messages, elapsed time).
 *
 * For more details about the flushing behavior, please see the class-level documentation on {@link MessageOutHandler}.
 */
public class OutboundMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);

    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";

    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /**
     * Describes this instance's ability to send messages into it's Netty {@link Channel}.
     */
    enum State
    {
        NOT_READY, CREATING_CHANNEL, READY, CLOSED
    }

    /**
     * Backlog to hold messages passed by upstream threads while the Netty {@link Channel} is being set up or recreated.
     */
    private final Queue<QueuedMessage> backlog;

    private final ScheduledExecutorService scheduledExecutor;

    final AtomicLong droppedMessageCount;
    final AtomicLong completedMessageCount;

    private volatile OutboundConnectionIdentifier connectionId;

    private final ServerEncryptionOptions encryptionOptions;

    /**
     * A future for notifying when the timeout for creating the connection and negotiating the handshake has elapsed.
     * It will be cancelled when the channel is established correctly. Bear in mind that this future does not execute in the
     * netty event event loop, so there's some races to be careful of.
     */
    private ScheduledFuture<?> connectionTimeoutFuture;

    /**
     * Borrowing a technique from netty: instead of using an {@link AtomicReference} for the {@link #state}, we can avoid a lot of garbage
     * allocation.
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<OutboundMessagingConnection, State> stateUpdater;

    static
    {
        stateUpdater = AtomicReferenceFieldUpdater.newUpdater(OutboundMessagingConnection.class, State.class, "state");
    }

    private volatile State state = State.NOT_READY;

    @Nullable
    private final CoalescingStrategy coalescingStrategy;

    private OutboundConnector outboundConnector;

    /**
     * The channel once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile OutChannel outChannel;

    /**
     * the target protocol version to communicate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                CoalescingStrategy coalescingStrategy)
    {
        this(connectionId, encryptionOptions, coalescingStrategy, ScheduledExecutors.scheduledFastTasks);
    }

    @VisibleForTesting
    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                CoalescingStrategy coalescingStrategy,
                                ScheduledExecutorService sceduledExecutor)
    {
        this.connectionId = connectionId;
        this.encryptionOptions = encryptionOptions;
        backlog = new ConcurrentLinkedQueue<>();
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        this.scheduledExecutor = sceduledExecutor;
        this.coalescingStrategy = coalescingStrategy;

        // We want to use the most precise protocol version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // only connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us, and in both cases that will set the version).
        // In that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(connectionId.remote());
    }

    /**
     * If the connection is set up and ready to use (the normal case), simply send the message to it and return.
     * Otherwise, one lucky thread is selected to create the Channel, while other threads just add the {@code msg} to
     * the backlog queue.
     */
    void sendMessage(MessageOut msg, int id)
    {
        QueuedMessage queuedMessage = new QueuedMessage(msg, id);

        if (state == State.READY)
        {
            outChannel.write(queuedMessage, this);
        }
        else
        {
            // TODO:JEB work out with pcmanus the best way to handle this
            backlog.add(queuedMessage);
            connect();
        }
    }

    /**
     * Sets the state properly so {@link #connect()} can attempt to reconnect.
     */
    void reconnect()
    {
        stateUpdater.set(this, State.NOT_READY);
        connect();
    }

    /**
     * Initiate all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}. However, this method will not block for all those actions: it will only
     * kick off the connection attempt via {@link OutboundConnector} as everything is asynchronous.
     * <p>
     * Threads compete to update the {@link #state} field to {@link State#CREATING_CHANNEL} to ensure only one
     * connection is attempted at a time.
     */
    public void connect()
    {
        // try to be the winning thread to create the channel
        if (state != State.NOT_READY || !stateUpdater.compareAndSet(this, State.NOT_READY, State.CREATING_CHANNEL))
            return;

        boolean compress = shouldCompressConnection(connectionId.remote());
        Bootstrap bootstrap = buildBootstrap(compress);
        outboundConnector = new OutboundConnector(bootstrap, connectionId);
        outboundConnector.connect();

        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        connectionTimeoutFuture = scheduledExecutor.schedule(() -> connectionTimeout(outboundConnector), timeout, TimeUnit.MILLISECONDS);
    }

    private Bootstrap buildBootstrap(boolean compress)
    {
        boolean tcpNoDelay = isLocalDC(connectionId.remote()) ? INTRADC_TCP_NODELAY : DatabaseDescriptor.getInterDCTcpNoDelay();
        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;
        int receiveBufferSize = DatabaseDescriptor.getInternodeRecvBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeRecvBufferSize()
                             : OutboundConnectionParams.DEFAULT_RECEIVE_BUFFER_SIZE;
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .callback(this::finishHandshake)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(Mode.MESSAGING)
                                                                  .compress(compress)
                                                                  .coalescingStrategy(coalescingStrategy)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .receiveBufferSize(receiveBufferSize)
                                                                  .tcpNoDelay(tcpNoDelay)
                                                                  .build();

        return NettyFactory.createOutboundBootstrap(params);
    }

    private boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(connectionId.local());
        return remoteDC != null && remoteDC.equals(localDC);
    }

    /**
     * A callback for handling timeouts when creating a connection.
     * <p>
     * Note: this method is *not* invoked from the netty event loop,
     * so there's an inherent race with {@link #finishHandshake(OutboundHandshakeHandler.HandshakeResult)},
     * as well as any possible connect() reattempts (a seemingly remote race condition, however).
     * Therefore, this function tries to lose any races, as much as possible.
     */
    void connectionTimeout(OutboundConnector initiatingConnector)
    {
        State initialState = state;
        if (initialState != State.READY)
        {
            // if we got this far, always cancel the connector
            initiatingConnector.cancel();

            // if the parameter initiatingConnector is the same as the member field,
            // no other thread has attempted a reconnect (and put a new instance into the member field)
            if (initiatingConnector == outboundConnector)
            {
                // a last-ditch attempt to let finishHandshake() win the race
                if (stateUpdater.compareAndSet(this, initialState, State.NOT_READY))
                    backlog.clear();
            }
        }
    }

    /**
     * Process the results of the handshake negotiation.
     * <p>
     * Note: this method will be invoked from the netty event loop,
     * so there's an inherent race with {@link #connectionTimeout(OutboundConnector)}.
     */
    void finishHandshake(OutboundHandshakeHandler.HandshakeResult result)
    {
        // clean up the connector instances before changing the state
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        outboundConnector = null;

        if (result.outcome != OutboundHandshakeHandler.HandshakeResult.Outcome.NEGOTIATION_FAILURE)
        {
            targetVersion = result.negotiatedMessagingVersion;
            MessagingService.instance().setVersion(connectionId.remote(), targetVersion);
        }

        switch (result.outcome)
        {
            case SUCCESS:
                logger.debug("successfully connected to {}, coalescing = {}", connectionId, coalescingStrategy != null);
                outChannel = result.outChannel;
                // TODO:JEB work out with pcmanus the best way to handle this
                // drain the backlog to the channel
                writeBacklogToChannel();
                // change the state so newly incoming messages can be sent to the channel (without adding to the backlog)
                stateUpdater.set(this, State.READY);
                // ship out any stragglers that got added to the backlog
                writeBacklogToChannel();
                break;
            case DISCONNECT:
                stateUpdater.set(this, State.NOT_READY);
                break;
            case NEGOTIATION_FAILURE:
                stateUpdater.set(this, State.NOT_READY);
                backlog.clear();
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.outcome);
        }
    }

    /**
     * Attempt to write the backlog of messages to the channel. Any backlogged {@link QueuedMessage}s are
     * not registered/logged with the {@link #coalescingStrategy} as those messages aren't behaving in the "normal"
     * path for coalescing - they were delayed due to setting up the socket connection, not the normal incoming rate,
     * so let's not unduly skew the {@link #coalescingStrategy}.
     */
    @VisibleForTesting
    void writeBacklogToChannel()
    {
        outChannel.writeBacklog(backlog, this);
    }

    private boolean shouldCompressConnection(InetAddress addr)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isLocalDC(addr));
    }

    int getTargetVersion()
    {
        return targetVersion;
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address, and
     * new incoming messages as well as existing {@link #backlog} messages will be sent there. Any outstanding messages
     * in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     */
    void reconnectWithNewIp(InetSocketAddress newAddr)
    {
        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        OutChannel currentChannel = outChannel;
        connectionId = connectionId.withNewConnectionAddress(newAddr);

        // kick off connecting on the new address. all new incoming messages will go that route, as well as any currently backlogged.
        reconnect();

        // lastly, push through anything remaining in the existing channel.
        if (currentChannel != null)
            currentChannel.softClose();
    }

    public void close(boolean softClose)
    {
        // close the connection creation objects before changing the state to avoid possible race conditions
        // on those member fields.
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        if (outboundConnector != null)
        {
            outboundConnector.cancel();
            outboundConnector = null;
        }

        stateUpdater.set(this, State.CLOSED);

        // drain the backlog
        if (outChannel != null)
        {
            if (softClose)
            {
                outChannel.writeBacklog(backlog, null);
                outChannel.softClose();
            }
            else
            {
                backlog.clear();
                outChannel.close();
            }
        }
    }

    @Override
    public String toString()
    {
        return connectionId.toString();
    }

    public Integer getPendingMessages()
    {
        int pending = backlog.size();
        OutChannel chan = outChannel;
        if (chan != null)
            pending += (int)chan.pendingMessageCount();
        return pending;
    }

    public Long getCompletedMessages()
    {
        return completedMessageCount.get();
    }

    public Long getDroppedMessages()
    {
        return droppedMessageCount.get();
    }

    /*
        methods specific to testing follow
     */

    @VisibleForTesting
    int backlogSize()
    {
        return backlog.size();
    }

    @VisibleForTesting
    void addToBacklog(QueuedMessage msg)
    {
        backlog.add(msg);
    }

    @VisibleForTesting
    void setOutChannel(OutChannel outChannel)
    {
        this.outChannel = outChannel;
    }

    @VisibleForTesting
    OutChannel getOutChannel()
    {
        return outChannel;
    }

    @VisibleForTesting
    void setState(State state)
    {
        this.state = state;
    }

    @VisibleForTesting
    State getState()
    {
        return state;
    }

    @VisibleForTesting
    void setOutboundConnector(OutboundConnector connector)
    {
        outboundConnector = connector;
    }

    @VisibleForTesting
    void setTargetVersion(int targetVersion)
    {
        this.targetVersion = targetVersion;
    }
}