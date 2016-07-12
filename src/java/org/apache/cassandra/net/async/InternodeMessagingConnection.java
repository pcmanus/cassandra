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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;
import org.apache.cassandra.net.OutboundTcpConnection.RetriedQueuedMessage;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.net.OutboundTcpConnection.isLocalDC;
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
public class InternodeMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(InternodeMessagingConnection.class);

    private static final int DEFAULT_BUFFER_SIZE = OutboundTcpConnection.BUFFER_SIZE;

    /**
     * Describes this instance's ability to send messages into it's Netty {@link Channel}.
     */
    enum State { NOT_READY, CREATING_CHANNEL, READY, CLOSED }

    /**
     * Backlog to hold messages passed by upstream threads while the Netty {@link Channel} is being set up or recreated.
     */
    private final Queue<QueuedMessage> backlog;

    private final ScheduledExecutorService scheduledExecutor;

    /**
     * A shared counter that tracks the number of yet-to-be-processed outbound messages.
     * Note: Acts similar to {@link OutboundTcpConnection#backlog#size} in that we look to see if there are
     * any more messages to be processed, and if not, flush any open buffer(s).
     *
     * To a degree, this can be seen as a proxy for the depth of the netty channel's message queue (which is not exposed).
     */
    private final AtomicLong outboundCount;

    private final AtomicLong droppedMessageCount;
    private final AtomicLong completedMessageCount;

    private final InetSocketAddress remoteAddr;
    private final InetSocketAddress localAddr;
    private final ServerEncryptionOptions encryptionOptions;
    private final CoalescingStrategy coalescingStrategy;
    private final int channelBufferSize;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP address
     * which need to be used for communication between EC2 regions.
     */
    private InetSocketAddress preferredRemoteAddr;

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
    private static final AtomicReferenceFieldUpdater<InternodeMessagingConnection, State> stateUpdater;
    static {
        @SuppressWarnings("rawtypes")
        AtomicReferenceFieldUpdater<InternodeMessagingConnection, State> referenceFieldUpdater = PlatformDependent.newAtomicReferenceFieldUpdater(InternodeMessagingConnection.class, "state");
        if (referenceFieldUpdater == null)
            referenceFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(InternodeMessagingConnection.class, State.class, "state");
        stateUpdater = referenceFieldUpdater;

    }
    private volatile State state = State.NOT_READY;

    private ClientConnector clientConnector;

    /**
     * The channel once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private Channel channel;

    /**
     * the target protocol version to communiate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    InternodeMessagingConnection(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions, CoalescingStrategy coalescingStrategy)
    {
        this(remoteAddr, localAddr, encryptionOptions, coalescingStrategy, ScheduledExecutors.scheduledTasks);
    }

    @VisibleForTesting
    InternodeMessagingConnection(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions, CoalescingStrategy coalescingStrategy, ScheduledExecutorService sceduledExecutor)
    {
        this.remoteAddr = remoteAddr;
        this.localAddr = localAddr;
        preferredRemoteAddr = remoteAddr;
        this.encryptionOptions = encryptionOptions;
        this.coalescingStrategy = coalescingStrategy;
        backlog = new ConcurrentLinkedQueue<>();
        outboundCount = new AtomicLong(0);
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        channelBufferSize = DEFAULT_BUFFER_SIZE;
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
     *
     * If the {@link #state} is {@link State#CLOSED}, just ignore the message.
     */
    public void enqueue(MessageOut msg, int id)
    {
        if (state == State.CLOSED)
            return;
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
        while (true)
        {
            if (!channel.isActive())
                return false;

            QueuedMessage backlogged = backlog.poll();
            if (backlogged == null)
                return true;

            // check to see if message has timed out, before shoveling it into the channel.
            if (backlogged.isTimedOut())
            {
                if (backlogged.shouldRetry())
                {
                    backlogged = new RetriedQueuedMessage(backlogged);
                }
                else
                {
                    droppedMessageCount.incrementAndGet();
                    continue;
                }
            }

            writeAndCount(backlogged);
        }
    }

    void writeAndCount(QueuedMessage msg)
    {
        outboundCount.incrementAndGet();
        ChannelFuture future = channel.write(msg);
        future.addListener(f -> handleMessagePromise(f, msg));
    }

    /**
     * Handles the result of attempting to send a message. If we've had an IOException, we typically want to create a new connection/channel.
     * Hence, we need a way of bounding the attempts per failed channel to reconnect as we could get into a weird
     * race where because the channel will call future.fail for each message in the dead channel (and hence invoke this callback),
     * we don't want all those callbacks to attempt to create a new channel.
     *
     * Note: this is called from the netty event loop, so it's safe to perform actions on the channel.
     */
    void handleMessagePromise(io.netty.util.concurrent.Future<? super Void> future, QueuedMessage msg)
    {
        if (!future.isDone() || future.isSuccess() || future.isCancelled())
            return;

        Throwable cause = future.cause();
        JVMStabilityInspector.inspectThrowable(cause);

        // all netty "connection" related exceptions seem to derive from IOException, so this is probably a safe check
        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            if (logger.isTraceEnabled())
                logger.trace("error writing to {}", preferredRemoteAddr, cause);

            // because we get the reference the channel to which the message was sent, we don't have to worry about
            // a race of the callback being invoked but the IMC already setting up a new channel (and thus we won't attempt to close that new channel)
            ChannelFuture channelFuture = (ChannelFuture) future;
            // check that it's safe to change the state (to kick off the reconnect); basically make sure the instance hasn't been closed
            // and that another thread hasn't already created a new channel. Basically, only the first message to fail on this channel
            // should trigger the reconnect.
            if (state == State.READY && channel == channelFuture.channel())
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
            logger.error("error writing to {}", preferredRemoteAddr, cause);
        }
    }

    /**
     * Sets the state properly so {@link #connect()} can attempt to reconnect.
     */
    private void reconnect()
    {
        if (state == State.CLOSED)
            return;
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
     * Perform all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}.
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

        // pass in the "broadcast address" here, and *not* the preferredRemoteAddr; preferredRemoteAddr is passed to ClientConnector
        clientConnector = new ClientConnector.Builder()
                          .addLocalAddr(localAddr)
                          .addRemoteAddr(preferredRemoteAddr)
                          .compress(shouldCompressConnection(remoteAddr.getAddress()))
                          .callback(this::finishHandshake)
                          .protocolVersion(messagingVersion)
                          .encryptionOptions(encryptionOptions)
                          .channelBufferSize(channelBufferSize)
                          .build();
        clientConnector.connect();

        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        connectionTimeoutFuture = scheduledExecutor.schedule(() -> connectionTimeout(clientConnector), timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * A callback for handling timeouts when cretaing connections.
     *
     * Note: this method is *not* invoked from the netty event loop,
     * so there's an inherent race with {@link #finishHandshake(ConnectionHandshakeResult)},
     * as well as any possible connect() reattempts (a seemingly remote race condition, however).
     */
    void connectionTimeout(ClientConnector initiatingConnector)
    {
        State initialState = state;
        if (initialState != State.READY)
        {
            // if we got this far, always cancel the connector/handler
            initiatingConnector.cancel();

            // if the parameter ClientConnector is the same as the same as the member field,
            // no other thread has attempted a reconnect (and put a new instance into the member field)
            if (initiatingConnector == clientConnector && initialState != State.CLOSED)
            {
                // a last-ditch attempt to let finishHandshake() win the any race
                if (stateUpdater.compareAndSet(this, initialState, State.NOT_READY))
                    backlog.clear();
            }
        }
    }

    /**
     * Process the results of the handshake negotiation.
     *
     * Note: this method will be invoked from the netty event loop,
     * so there's an inherent race with {@link #connectionTimeout(ClientConnector)}.
     */
    void finishHandshake(ConnectionHandshakeResult result)
    {
        // clean up the connector instances before changing the state
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        clientConnector = null;

        if (result.result != ConnectionHandshakeResult.Result.NEGOTIATION_FAILURE)
        {
            MessagingService.instance().setVersion(remoteAddr.getAddress(), result.negotiatedMessagingVersion);
            targetVersion = result.negotiatedMessagingVersion;
        }
        channel = result.channel;

        switch (result.result)
        {
            case GOOD:
                setupPipeline(result.channel.pipeline(), result.negotiatedMessagingVersion, shouldCompressConnection(remoteAddr.getAddress()));
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
                backlog.clear();
                stateUpdater.set(this, State.NOT_READY);
                break;
            default:
                throw new IllegalArgumentException("unhndled result type: " + result.result);
        }
    }

    void setupPipeline(ChannelPipeline pipeline, int messagingVersion, boolean compress)
    {
        if (compress)
            pipeline.addLast("outboundCompressor", new Lz4FrameEncoder());

        pipeline.addLast("messageOutHandler", new MessageOutHandler(messagingVersion, completedMessageCount, channelBufferSize));
        pipeline.addLast(COALESCING_MESSAGE_CHANNEL_HANDLER_NAME, new CoalescingMessageOutHandler(coalescingStrategy, outboundCount, droppedMessageCount));
    }

    private static boolean shouldCompressConnection(InetAddress addr)
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
     * in the existing {@link #channel} will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     */
    void switchIpAddress(InetSocketAddress newAddr)
    {
        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        Channel currentChannel = channel;
        preferredRemoteAddr = newAddr;

        // kick off connecting on the new address. all new incoming messages will go that route, as well as any currently backlogged.
        reconnect();

        // lastly, push through anything remaining in the existing channel.
        // the netty folks advised to write and flush something to the channel, and then add a listener close the channel
        // once the last message/buffer is written to the socket. The trick is we really don't know what to write as we don't
        // have a friendly 'close this socket' message.
        currentChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    public void close()
    {
        stateUpdater.set(this, State.CLOSED);

        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        if (clientConnector != null)
        {
            clientConnector.cancel();
            clientConnector = null;
        }

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
        return (int)(backlog.size() + outboundCount.get());
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
    int getOutboundCount()
    {
        return outboundCount.intValue();
    }

    @VisibleForTesting
    void setClientConnector(ClientConnector connector)
    {
        clientConnector = connector;
    }

    @VisibleForTesting
    void setTargetVersion(int targetVersion)
    {
        this.targetVersion = targetVersion;
    }
}
