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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.NettyFactory.OutboundChannelInitializer;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
 * Note: when sending a message to the netty {@link Channel}, we call {@link Channel#writeAndFlush(Object)} versus
 * {@link Channel#write(Object)} because, at least as of netty 4.1.8, {@link Channel#writeAndFlush(Object)} causes the
 * netty event thread to be woken up, whereas {@link Channel#write(Object)} does not wake up the thread. The problem
 * becomes that without the thread being woken up, the write is not processed immediately and processing latency is
 * introduced.
 */
public class OutboundMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);

    enum ConnectionType { GOSSIP, LARGE_MESSAGE, SMALL_MESSAGE }

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

    private final ConnectionType connectionType;
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
    private static final AtomicIntegerFieldUpdater<OutboundMessagingConnection> scheduledFlushStateUpdater;

    static
    {
        stateUpdater = AtomicReferenceFieldUpdater.newUpdater(OutboundMessagingConnection.class, State.class, "state");
        scheduledFlushStateUpdater = AtomicIntegerFieldUpdater.newUpdater(OutboundMessagingConnection.class, "scheduledFlushState");
    }

    private volatile State state = State.NOT_READY;
    private volatile int scheduledFlushState;

    private final CoalescingStrategy coalescingStrategy;
    private final boolean isCoalescing;

    private OutboundConnector outboundConnector;

    /**
     * The channel once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile ChannelWrapper channelWrapper;

    /**
     * the target protocol version to communiate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    OutboundMessagingConnection(ConnectionType connectionType, InetSocketAddress remoteAddr, InetSocketAddress localAddr,
                                ServerEncryptionOptions encryptionOptions, CoalescingStrategy coalescingStrategy)
    {
        this(connectionType, remoteAddr, localAddr, encryptionOptions, coalescingStrategy, ScheduledExecutors.scheduledFastTasks);
    }

    @VisibleForTesting
    OutboundMessagingConnection(ConnectionType connectionType, InetSocketAddress remoteAddr, InetSocketAddress localAddr,
                                ServerEncryptionOptions encryptionOptions, CoalescingStrategy coalescingStrategy, ScheduledExecutorService sceduledExecutor)
    {
        this.connectionType = connectionType;
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        preferredConnectAddress = remoteAddr;
        this.encryptionOptions = encryptionOptions;
        backlog = new ConcurrentLinkedQueue<>();
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        this.scheduledExecutor = sceduledExecutor;
        this.coalescingStrategy = coalescingStrategy;
        isCoalescing = coalescingStrategy.isCoalescing();

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
     * If the {@link #channelWrapper#channel} is set up and ready to use (the normal case), simply send the message to it and return.
     * If the {@link #channelWrapper#channel} is not set up, then one lucky thread is selected to create the Channel, while other threads
     * just add the {@code msg} to the backlog queue.
     */
    void sendMessage(MessageOut msg, int id)
    {
        QueuedMessage queuedMessage = new QueuedMessage(msg, id);

        if (state == State.READY)
        {
            // grab a local referene to the member field, in case it changes while we execute -
            // mostly for the async coalesced flush
            ChannelWrapper wrapper = this.channelWrapper;
            wrapper.pendingMessageCount.incrementAndGet();
            final Channel channelLocal = wrapper.channel;

            if (!isCoalescing)
            {
                ChannelFuture future = channelLocal.writeAndFlush(queuedMessage);
                future.addListener(f -> handleMessageFuture(f, queuedMessage));
                return;
            }

            coalescingStrategy.newArrival(queuedMessage);

            // TODO:JEB there may be some race conditions here
            // if we lost the race to set the state, simply write to the channel (no flush)
            if (!(scheduledFlushStateUpdater.compareAndSet(this, 0, 1)))
            {
                ChannelFuture future = channelLocal.write(queuedMessage);
                future.addListener(f -> handleMessageFuture(f, queuedMessage));
                return;
            }

            long flushDelayNanos = coalescingStrategy.currentCoalescingTimeNanos();
            // if we've run out of coalesce time, write and flush
            if (flushDelayNanos <= 0)
            {
                scheduledFlushStateUpdater.set(this, 0);
                ChannelFuture future = channelLocal.writeAndFlush(queuedMessage);
                future.addListener(f -> handleMessageFuture(f, queuedMessage));
                return;
            }

            ChannelFuture future = channelLocal.write(queuedMessage);
            future.addListener(f -> handleMessageFuture(f, queuedMessage));

            // calling schedule() on the eventLoop will force it to wake up (it not already executing) and schedule the task
            channelLocal.eventLoop().schedule(() -> {
                scheduledFlushStateUpdater.set(this, 0);
                // we then execute() the flush() like this because netty runs the scheduled tasks before consuming from
                // it's queue, which means it would process the flush() before any of the write tasks if we had just called
                // flush() directly. by submitting via eventLoop().execute(), we ensure the flush task is enqueued
                // at the end (after the write tasks).
                channelLocal.eventLoop().execute(() -> channelLocal.flush());
            }, flushDelayNanos, TimeUnit.NANOSECONDS);        }
        else
        {
            // TODO:JEB work out with pcmanus the best way to handle this
            backlog.add(queuedMessage);
            connect();
        }
    }

    /**
     * Handles the result of attempting to send a message. If we've had an IOException, we typically want to create a new connection/channel.
     * Hence, we need a way of bounding the attempts per failed channel to reconnect as we could get into a weird
     * race where because the channel will call future.fail for each message in the dead channel (and hence invoke this callback),
     * we don't want all those callbacks to attempt to create a new channel.
     * <p>
     * Note: this is called from the netty event loop, so it's safe to perform actions on the channel.
     */
    void handleMessageFuture(io.netty.util.concurrent.Future<? super Void> future, QueuedMessage msg)
    {
        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        Throwable cause = future.cause();
        if (cause == null)
            return;

        JVMStabilityInspector.inspectThrowable(cause);

        boolean requeue = false;
        if (future.isCancelled() || cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            logger.trace("error writing to peer {} (at address {}). error: {}", remoteAddr, preferredConnectAddress, cause);

            // because we get the reference the channel to which the message was sent, we don't have to worry about
            // a race of the callback being invoked but the OMC already setting up a new channel (and thus we won't attempt to close that new channel)
            ChannelFuture channelFuture = (ChannelFuture) future;
            // check that it's safe to change the state (to kick off the reconnect); basically make sure the instance hasn't been closed
            // and that another thread hasn't already created a new channel. Basically, only the first message to fail on this channel
            // should trigger the reconnect.
            if (state == State.READY && channelWrapper.channel.id().equals(channelFuture.channel().id()))
            {
                reconnect();
                channelFuture.channel().close();
            }

            if (msg.shouldRetry())
                requeue = true;
        }
        // ExpiredException is thrown when the message sits in the queue/channel for too long before being sent
        else if (cause instanceof ExpiredException && msg.shouldRetry())
        {
            requeue = true;
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("error writing to peer {} (on address {})", remoteAddr, preferredConnectAddress, cause);
        }

        if (requeue)
        {
            sendMessage(msg.message, msg.id);
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
     * Intiate all the actions required to establish a working, valid connection. This includes
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
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, preferredConnectAddress, messagingVersion,
                                                                       this::finishHandshake, encryptionOptions, Mode.MESSAGING,
                                                                       compress, coalescingStrategy.isCoalescing(), droppedMessageCount,
                                                                       completedMessageCount, new AtomicLong(0), connectionType);
        OutboundChannelInitializer initializer = new OutboundChannelInitializer(params);

        boolean tcpNoDelay = isLocalDC(remoteAddr.getAddress()) ? INTRADC_TCP_NODELAY : DatabaseDescriptor.getInterDCTcpNoDelay();

        int sendBufferSize = 1 << 16;
        if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
            sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize();

        return NettyFactory.createOutboundBootstrap(initializer, sendBufferSize, tcpNoDelay);
    }

    private boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(localAddr.getAddress());
        return remoteDC != null && remoteDC.equals(localDC);
    }

    /**
     * A callback for handling timeouts when creating a connection.
     * <p>
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
     * <p>
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
            targetVersion = result.negotiatedMessagingVersion;
            MessagingService.instance().setVersion(remoteAddr.getAddress(), targetVersion);
        }
        Channel channel = result.channel;

        switch (result.result)
        {
            case GOOD:
                logger.debug("successfully connected to {} for connection type {}, coalescing = {}", remoteAddr, connectionType, isCoalescing);
                channelWrapper = new ChannelWrapper(channel, result.pendingMessages);
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
                if (channel != null)
                    channel.close();
                break;
            case NEGOTIATION_FAILURE:
                stateUpdater.set(this, State.NOT_READY);
                if (channel != null)
                    channel.close();
                backlog.clear();
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.result);
        }
    }

    /**
     * Attempt to write the backlog of messages to the channel. Any backlogged {@link QueuedMessage}s are
     * not registered/logged with the {@link #coalescingStrategy} as those messages aren't behaving in the "normal"
     * path for coalescing - they were delayed due to setting up the socket connection, not the normal incoming rate,
     * so let's not unduly skew the {@link #coalescingStrategy}.
     */
    void writeBacklogToChannel()
    {
        boolean wroteOnce = false;
        final Channel channel = channelWrapper.channel;
        final AtomicLong pendingMessages = channelWrapper.pendingMessageCount;
        while (true)
        {
            final QueuedMessage msg = backlog.poll();
            if (msg == null)
                break;
            pendingMessages.incrementAndGet();
            ChannelFuture future = channel.write(msg);
            future.addListener(f -> handleMessageFuture(f, msg));
            wroteOnce = true;
        }

        // as this is an infrequent operation, don't bother coordinating with the instance-level flush task
        if (wroteOnce)
            channel.flush();
    }

    private boolean shouldCompressConnection(InetAddress addr)
    {
        // assumes version >= 1.2
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
        Channel currentChannel = channelWrapper.channel;
        preferredConnectAddress = newAddr;

        // kick off connecting on the new address. all new incoming messages will go that route, as well as any currently backlogged.
        reconnect();

        // lastly, push through anything remaining in the existing channel.
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

        stateUpdater.set(this, State.CLOSED);

        // drain the backlog
        ChannelWrapper wrapper = this.channelWrapper;
        Object o;
        while ((o = backlog.poll()) != null)
        {
            if (softClose && wrapper != null && wrapper.channel != null)
                wrapper.channel.write(o);
        }

        if (wrapper != null && wrapper.channel != null)
            wrapper.channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
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
        enum Result
        {
            GOOD, DISCONNECT, NEGOTIATION_FAILURE
        }

        public final Channel channel;
        public final int negotiatedMessagingVersion;
        public final Result result;
        public final AtomicLong pendingMessages;

        ConnectionHandshakeResult(Channel channel, int negotiatedMessagingVersion, Result result, AtomicLong pendingMessages)
        {
            this.channel = channel;
            this.negotiatedMessagingVersion = negotiatedMessagingVersion;
            this.result = result;
            this.pendingMessages = pendingMessages;
        }

        static ConnectionHandshakeResult failed()
        {
            return new ConnectionHandshakeResult(null, -1, Result.NEGOTIATION_FAILURE, null);
        }
    }

    public Integer getPendingMessages()
    {
        int pending = backlog.size();
        ChannelWrapper channelWrapper = this.channelWrapper;
        pending += channelWrapper != null ? channelWrapper.pendingMessageCount.intValue() : 0;
        return pending;
    }

    public Long getCompletedMesssages()
    {
        return completedMessageCount.get();
    }

    public Long getDroppedMessages()
    {
        return droppedMessageCount.get();
    }

    /**
     * A simple struct to wrap the {@link Channel} and any other channel-specific things we might need.
     */
    private static final class ChannelWrapper
    {
        private final Channel channel;

        /**
         * The number of currently pending messages on this channel.
         */
        private final AtomicLong pendingMessageCount;

        private ChannelWrapper(Channel channel, AtomicLong pendingMessageCount)
        {
            this.channel = channel;
            this.pendingMessageCount = pendingMessageCount;
        }
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
        this.channelWrapper = new ChannelWrapper(channel, new AtomicLong());
    }

    @VisibleForTesting
    Channel getChannel()
    {
        return channelWrapper.channel;
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

    @VisibleForTesting
    void setPendingMessages(int i)
    {
        channelWrapper.pendingMessageCount.set(i);
    }
}