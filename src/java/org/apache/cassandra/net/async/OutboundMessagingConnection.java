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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.ConnectionUtils;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.NettyFactory.OutboundChannelInitializer;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.net.async.NettyFactory.COALESCING_MESSAGE_CHANNEL_HANDLER_NAME;

/**
 * Represents one connection to a peer, and handles the state transistions on the connection and the netty {@link Channel}
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * The basic setup for the channel is like this: a message is requested to be sent via {@link #enqueue(MessageOut, int)}.
 * If the channel is not established, then we need to create it (obviously). To prevent multiple threads from creating
 * independent connections, they attempt to update the {@link #state} via the {@link #stateUpdater}; one thread will win the race
 * and create the connection. Upon sucessfully setting up the connection/channel, the {@link #state} will be updated again
 * (to {@link State#READY}, which indicates to other threads that the channel is ready for business and can be written to.
 */
public class OutboundMessagingConnection
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);

    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";
    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    private static final String BUFFER_SIZE_PROPERTY = Config.PROPERTY_PREFIX + "otc_buffer_size";
    /**
     * Size of the buffer for data being sent out. This is the buffer that we serialize messages into, which is not the same as the
     * socket send buffer (whose size is controlled by {@link DatabaseDescriptor#getInternodeSendBufferSize()}).
     */
    static final int DEFAULT_BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, 1024 * 64);

    /**
     * Describes this instance's ability to send messages into it's Netty {@link Channel}.
     */
    enum State { NOT_READY, CREATING_CHANNEL, READY }

    /**
     * Backlog to hold messages passed by upstream threads while the Netty {@link Channel} is being set up or recreated.
     */
    private final Queue<QueuedMessage> backlog;

    private final ScheduledExecutorService scheduledExecutor;

    private final AtomicLong droppedMessageCount;
    private final AtomicLong completedMessageCount;

    /**
     * Memoization of the local node's broadcast address.
     */
    private final InetSocketAddress localAddr;

    /**
     * An identifier for the peer. Use {@link #preferredConnectAddress} as the address to actually connect on.
     */
    private final InetSocketAddress remoteAddr;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP address
     * which need to be used for communication between EC2 regions.
     */
    private volatile InetSocketAddress preferredConnectAddress;

    private final ServerEncryptionOptions encryptionOptions;
    private final boolean maybeCoalesce;

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
    static {
        @SuppressWarnings("rawtypes")
        AtomicReferenceFieldUpdater<OutboundMessagingConnection, State> referenceFieldUpdater = PlatformDependent.newAtomicReferenceFieldUpdater(OutboundMessagingConnection.class, "state");
        if (referenceFieldUpdater == null)
            referenceFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(OutboundMessagingConnection.class, State.class, "state");
        stateUpdater = referenceFieldUpdater;

    }
    private volatile State state = State.NOT_READY;

    private OutboundConnector outboundConnector;

    /**
     * The channel once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile Channel channel;

    /**
     * the target protocol version to communiate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    OutboundMessagingConnection(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions, boolean maybeCoalesce)
    {
        this(remoteAddr, localAddr, encryptionOptions, maybeCoalesce, ScheduledExecutors.scheduledTasks);
    }

    @VisibleForTesting
    OutboundMessagingConnection(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions, boolean maybeCoalesce, ScheduledExecutorService sceduledExecutor)
    {
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        preferredConnectAddress = remoteAddr;
        this.encryptionOptions = encryptionOptions;
        this.maybeCoalesce = maybeCoalesce;
        backlog = new ConcurrentLinkedQueue<>();
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        this.scheduledExecutor = sceduledExecutor;

        // We want to use the most precise protocol version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // only connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us, and in both cases that will set the version).
        // In that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(remoteAddr.getAddress());
    }

    /**
     * If the {@link #channel} is set up and ready to use (the normal case), simply send the message to it and return.
     * If the {@link #channel} is not set up, then one lucky thread is selected to create the Channel, while other threads
     * just add the {@code msg} to the backlog queue.
     */
    void enqueue(MessageOut msg, int id)
    {
        backlog.add(new QueuedMessage(msg, id));
        if (state == State.READY)
            writeBacklogToChannel();
        else
            connect();
    }

    /**
     * Attempt to write the backlog of messages to the {@link #channel}.
     * @return true if the backlog is empty when this method completes; else, false.
     */
    boolean writeBacklogToChannel()
    {
        boolean wroteMessage = false;
        while (true)
        {
            final QueuedMessage backlogged = backlog.poll();
            if (backlogged == null)
            {
                if (wroteMessage)
                    channel.flush();
                return true;
            }

            // check to see if message has timed out, before shoveling it into the channel.
            if (!backlogged.isTimedOut())
            {
                ChannelFuture future = channel.write(backlogged);
                future.addListener(f -> handleMessageFuture(f, backlogged));
                wroteMessage = true;
            }
            else
            {
                if (backlogged.shouldRetry())
                {
                    final RetriedQueuedMessage retriedQueuedMessage = new RetriedQueuedMessage(backlogged);
                    ChannelFuture future = channel.write(retriedQueuedMessage);
                    future.addListener(f -> handleMessageFuture(f, retriedQueuedMessage));
                    wroteMessage = true;
                }
                else
                {
                    droppedMessageCount.incrementAndGet();
                    continue;
                }
            }
        }
    }

    /**
     * Handles the result of attempting to send a message. If we've had an IOException, we typically want to create a new connection/channel.
     * Hence, we need a way of bounding the attempts per failed channel to reconnect as we could get into a weird
     * race where because the channel will call future.fail for each message in the dead channel (and hence invoke this callback),
     * we don't want all those callbacks to attempt to create a new channel.
     *
     * Note: this is called from the netty event loop, so it's safe to perform actions on the channel.
     */
    void handleMessageFuture(io.netty.util.concurrent.Future<? super Void> future, QueuedMessage msg)
    {
        // only handle failures, for now
        if (future.cause() == null)
            return;

        Throwable cause = future.cause();
        JVMStabilityInspector.inspectThrowable(cause);

        // all netty "connection" related exceptions seem to derive from IOException, so this is probably a safe check

        // TODO:JEB handle "cause instanceof CancellationException"
        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            logger.trace("error writing to peer {} (at address {})", remoteAddr, preferredConnectAddress, cause);

            // because we get the reference the channel to which the message was sent, we don't have to worry about
            // a race of the callback being invoked but the IMC already setting up a new channel (and thus we won't attempt to close that new channel)
            ChannelFuture channelFuture = (ChannelFuture) future;
            // check that it's safe to change the state (to kick off the reconnect); basically make sure the instance hasn't been closed
            // and that another thread hasn't already created a new channel. Basically, only the first message to fail on this channel
            // should trigger the reconnect.
            if (state == State.READY && channel.id().equals(channelFuture.channel().id()))
            {
                // there's a subtle timing issue here. we need to move the messages out of CMOH before closing the channel,
                // but we also need to stop writing new messages to the channel.
                reconnect();

                movePendingCoalecsedMessages(channelFuture.channel());
                channelFuture.channel().close();
            }

            if (msg.shouldRetry())
                backlog.add(new RetriedQueuedMessage(msg));
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("error writing to peer {} (on address {})", remoteAddr, preferredConnectAddress, cause);
        }
    }

    /**
     * Sets the state properly so {@link #connect()} can attempt to reconnect.
     */
    private void reconnect()
    {
        stateUpdater.set(this, State.NOT_READY);
        connect();
    }

    /**
     * Salvage any coalscing messages from the channel's {@link CoalescingMessageOutHandler}, if it has one,
     * and put them into the {@link #backlog}.
     */
    private void movePendingCoalecsedMessages(Channel currentChannel)
    {
        CoalescingMessageOutHandler cmoh = (CoalescingMessageOutHandler) currentChannel.pipeline().get(COALESCING_MESSAGE_CHANNEL_HANDLER_NAME);
        if (cmoh != null)
        {
            cmoh.setClosed();
            for (Iterator<QueuedMessage> iter = cmoh.iterator(); iter.hasNext(); )
                backlog.add(iter.next());
        }
    }

    /**
     * Intiate all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}. However, this method will not block for all those actions: it will only
     * kick off the connection attempt via {@link OutboundConnector} as everything is asynchronous.
     *
     * Threads compete to update the {@link #state} field to {@link State#CREATING_CHANNEL} to ensure only one
     * connection is attempted at a time.
     */
    public void connect()
    {
        // try to be the winning thread to create the channel
        if (!stateUpdater.compareAndSet(this, State.NOT_READY, State.CREATING_CHANNEL))
            return;

        int messagingVersion = MessagingService.instance().getVersion(remoteAddr.getAddress());
        boolean compress = shouldCompressConnection(remoteAddr.getAddress());
        Bootstrap bootstrap = buildBootstrap(messagingVersion, compress);
        outboundConnector = new OutboundConnector(bootstrap, localAddr, preferredConnectAddress);
        outboundConnector.connect();

        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        connectionTimeoutFuture = scheduledExecutor.schedule(() -> connectionTimeout(outboundConnector), timeout, TimeUnit.MILLISECONDS);
    }

    private Bootstrap buildBootstrap(int messagingVersion, boolean compress)
    {
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, preferredConnectAddress, messagingVersion, DEFAULT_BUFFER_SIZE,
                                                                       this::finishHandshake, encryptionOptions, Mode.MESSAGING,
                                                                       maybeCoalesce, compress, droppedMessageCount, completedMessageCount);
        OutboundChannelInitializer initializer = new OutboundChannelInitializer(params);

        boolean tcpNoDelay = ConnectionUtils.isLocalDC(remoteAddr.getAddress()) ?
                             INTRADC_TCP_NODELAY :
                             DatabaseDescriptor.getInterDCTcpNoDelay();

        int sendBufferSize = 1 << 16;
        if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
            sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize();

        return NettyFactory.createOutboundBootstrap(initializer, sendBufferSize, tcpNoDelay);
    }

    /**
     * A callback for handling timeouts when creating a connection.
     *
     * Note: this method is *not* invoked from the netty event loop,
     * so there's an inherent race with {@link #finishHandshake(ConnectionHandshakeResult)},
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

            // if the parameter initiatingConnector is the same as the same as the member field,
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
     *
     * Note: this method will be invoked from the netty event loop,
     * so there's an inherent race with {@link #connectionTimeout(OutboundConnector)}.
     */
    void finishHandshake(ConnectionHandshakeResult result)
    {
        // clean up the connector instances before changing the state
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        outboundConnector = null;

        if (result.result != ConnectionHandshakeResult.Result.NEGOTIATION_FAILURE)
        {
            MessagingService.instance().setVersion(remoteAddr.getAddress(), result.negotiatedMessagingVersion);
            targetVersion = result.negotiatedMessagingVersion;
        }
        channel = result.channel;

        switch (result.result)
        {
            case GOOD:
                // change the state so incoming messages can be sent to the channel (without adding to the backlog)
                stateUpdater.set(this, State.READY);
                scheduledExecutor.execute(this::writeBacklogToChannel);
                break;
            case DISCONNECT:
                if (channel != null)
                    channel.close();
                stateUpdater.set(this, State.NOT_READY);
                break;
            case NEGOTIATION_FAILURE:
                if (channel != null)
                    channel.close();
                backlog.clear();
                stateUpdater.set(this, State.NOT_READY);
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.result);
        }
    }

    static boolean shouldCompressConnection(InetAddress addr)
    {
        // assumes version >= 1.2
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !ConnectionUtils.isLocalDC(addr));
    }

    int getTargetVersion()
    {
        return targetVersion;
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address, and
     * new incoming messages as well as existing {@link #backlog} messages will be sent there. Any outstanding messages
     * in the existing {@link #channel} will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     */
    void reconnectWithNewIp(InetSocketAddress newAddr)
    {
        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        Channel currentChannel = channel;
        preferredConnectAddress = newAddr;

        // kick off connecting on the new address. all new incoming messages will go that route, as well as any currently backlogged.
        reconnect();

        // lastly, push through anything remaining in the existing channel.
        // the netty folks advised to write and flush something to the channel, and then add a listener close the channel
        // once the last message/buffer is written to the socket. The trick is we really don't know what to write as we don't
        // have a friendly 'close this socket' message.
        if (currentChannel != null)
            currentChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
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

        stateUpdater.set(this, State.NOT_READY);

        if (!softClose)
            backlog.clear();

        if (channel != null)
            channel.close();
    }

    /**
     * A simple class to hold the result of completed connection attempt.
     */
    static class ConnectionHandshakeResult
    {
        /**
         * Describes the result of receiving the response back from the peer (Message 2 of the handshake)
         * and implies an action that should be taken.
         */
        enum Result { GOOD, DISCONNECT, NEGOTIATION_FAILURE }

        public final Channel channel;
        public final int negotiatedMessagingVersion;
        public final Result result;

        ConnectionHandshakeResult(Channel channel, int negotiatedMessagingVersion, Result result)
        {
            this.channel = channel;
            this.negotiatedMessagingVersion = negotiatedMessagingVersion;
            this.result = result;
        }

        static ConnectionHandshakeResult failed()
        {
            return new ConnectionHandshakeResult(null, -1, Result.NEGOTIATION_FAILURE);
        }
    }

    /**
     * Get pending messages count; not guaranteed to be a constant-time call.
     */
    public Integer getPendingMessages()
    {
        //TODO:JEB this might not be completely accurate as it doen't account for anything in the netty channel itself
        return backlog.size();
    }

    public Long getCompletedMesssages()
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
    void setChannel(Channel channel)
    {
        this.channel = channel;
    }

    @VisibleForTesting
    Channel getChannel()
    {
        return channel;
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
