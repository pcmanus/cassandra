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
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.MessageSizeEstimator;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Represents a ready and post-handshake channel to send outbound messages.
 * <p>
 * This class groups a netty channel with any other channel-related information we track and, most importantly, handles
 * the details on when the channel is flushed.
 *
 * <h2>Flushing</h2>
 *
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call,  copying
 * the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data (for some definition
 * of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * <p>
 * When to flush mainly depends on whether we use message coalescing or not ({@link CoalescingStrategies}).
 *
 * <h3>Flushing without coalescing</h3>
 *
 * When no coalescing is in effect, we want to send new message "right away". However, as said above, flushing after
 * every message would be particularly inefficient when there is lots of message in our sending queue, and so in
 * practice we want to flush in 2 cases:
 *  1) After any message <b>if</b> there is no pending message in the send queue.
 *  2) When we've filled up or exceeded the netty outbound buffer ({@link ChannelOutboundBuffer})
 * <p>
 * The second part is relatively simple and handled generically in {@link MessageOutHandler#channelWritabilityChanged} [1].
 * The first part however is made a little more complicated by how netty's event loop executes. It is woken up by
 * external callers to the channel invoking a flush, either {@link Channel#flush} or one of the {@link Channel#writeAndFlush}
 * methods. So a plain {@link Channel#write} will only queue the message in the channel, and not wake up the event loop.
 * <p>
 * This means we don't want to simply call {@link Channel#write} as we want the message processed immediately. But we
 * also don't want to flush on every message if there is more in the sending queue, so simply calling
 * {@link Channel#writeAndFlush} isn't completely appropriate either. In practice, we handle this by calling
 * {@link Channel#writeAndFlush} (so the netty even loop <b>does</b> wake up), but we override the flush behavior so
 * it actually only flush if there is no pending messages (see how {@link MessageOutHandler#flush} delegates the flushing
 * decision to this class through {@link #onTriggeredFlush}, and how {@link SimpleOutChannel} makes this a no-op;
 * instead {@link SimpleOutChannel} flushes after any message if their is no more pending ones in
 * {@link #onMessageProcessed}).
 *
 * <h3>Flushing with coalescing</h3>
 *
 * The goal of coalescing is to (artificially) delay the flushing of data in order to aggregate even more data before
 * sending a group of packets out. So we don't want to flush after messages even if there is no pending messages in the
 * sending queue, but we rather want to delegate the decision on when to flush to the {@link CoalescingStrategy}. In
 * pratice, when coalescing is enabled we will flush in 2 cases:
 *  1) When the coalescing strategies decides that we should.
 *  2) When we've filled up or exceeded the netty outbound buffer ({@link ChannelOutboundBuffer}), exactly like in the
 *  no coalescing case.
 *  <p>
 *  The second part is handled exactly like in the no coalescing case, see above.
 *  The first part is handled by {@link CoalescingOutChannel#write(QueuedMessage)}. Whenever a message is sent, we check
 *  if a flush has been already scheduled by the coalescing strategy. If one has, we're done, otherwise we ask the
 *  strategy when the next flush should happen and schedule one.
 *
 *
 * [1] For those desperately interested, and only after you've read the entire class-level doc: You can register a custom
 * {@link MessageSizeEstimator} with a netty channel. When a message is written to the channel, it will check the
 * message size, and if the max ({@link ChannelOutboundBuffer}) size will be exceeded, a task to signal the "channel
 * writability changed" will be executed in the channel. That task, however, will wake up the event loop.
 * Thus if coalescing is enabled, the event loop will wake up prematurely and process (and flush!) the messages
 * currently in the queue, thus defeating an aspect of coalescing. Hence, we're not using that feature.
 * [2]: It is also woken up by it's own internal timeout on the epoll_wait() system call.
 */
abstract class OutChannel
{
    private static final Logger logger = LoggerFactory.getLogger(OutChannel.class);

    protected final Channel channel;
    private volatile boolean closed;

    /** Number of currently pending messages on this channel. */
    final AtomicLong pendingMessageCount = new AtomicLong(0);

    protected OutChannel(Channel channel)
    {
        this.channel = channel;
    }

    /**
     * Creates a new {@link OutChannel} using the (assumed properly connected) provided channel, and using coalescing
     * based on the provided strategy.
     */
    static OutChannel create(Channel channel, CoalescingStrategy coalescingStrategy)
    {
        return coalescingStrategy.isCoalescing()
               ? new CoalescingOutChannel(channel, coalescingStrategy)
               : new SimpleOutChannel(channel);
    }

    long pendingMessageCount()
    {
        return pendingMessageCount.get();
    }

    /**
     * Handles the future of sending a particular message on this {@link OutChannel}.
     * <p>
     * Note: this is called from the netty event loop, so there is no race across multiple execution of this method.
     */
    @VisibleForTesting
    void handleMessageFuture(Future<? super Void> future, QueuedMessage msg, OutboundMessagingConnection connection)
    {
        connection.completedMessageCount.incrementAndGet();

        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        // Note that ExpiredException is just a marker for timeout-ed message we're dropping, but as we already
        // incremented the dropped message count in MessageOutHandler, we have nothing to do.
        Throwable cause = future.cause();
        if (cause == null)
            return;

        if (cause instanceof ExpiredException)
        {
            connection.droppedMessageCount.incrementAndGet();
            return;
        }

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            // This OutChannel needs to be closed and we need to trigger a reconnection. We really only want to do that
            // once for this channel however (and again, no race because we're on the netty event loop).
            if (!closed)
            {
                connection.reconnect();
                close();
            }

            if (msg.shouldRetry())
                connection.sendMessage(msg.message, msg.id);
        }
        else if (future.isCancelled())
        {
            // Someone cancelled the future, which we assume meant it doesn't want the message to be sent if it hasn't
            // yet. Just ignore.
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("Unexpected error writing on " + connection, cause);
        }
    }

    /**
     * Writes a message to this {@link OutChannel}.
     *
     * @param message the message to write/send.
     * @param connection the connection this {@link OutChannel} is part of. This is used to increment completed/dropped
     * messages counts and to trigger reconnects if error occurs rendering this channel unusable.
     */
    void write(QueuedMessage message, OutboundMessagingConnection connection)
    {
        write(message).addListener(f -> handleMessageFuture(f, message, connection));
    }

    /**
     * Writes a backlog of message to this {@link OutChannel}. This is mostly equivalent to calling
     * {@link #write(QueuedMessage, OutboundMessagingConnection)} for every message of the provided backlog queue, but
     * it ignores any coalescing, triggering a flush only once after all messages have been sent.
     *
     * @param backlog the backlog of message to send.
     * @param connection the connection this {@link OutChannel} is part of. Its use is the same than in
     * {@link #write(QueuedMessage, OutboundMessagingConnection)}, but this can actually be {@code null} if one doesn't
     * want any side effect on the connection (mainly used when closing the connection "softly" and we want to attempt
     * sending backlogged messages but don't want to end up reconnecting on errors).
     */
    void writeBacklog(Queue<QueuedMessage> backlog, OutboundMessagingConnection connection)
    {
        boolean wroteOnce = false;
        while (true)
        {
            QueuedMessage msg = backlog.poll();
            if (msg == null)
                break;

            pendingMessageCount.incrementAndGet();
            ChannelFuture future = channel.write(msg);
            if (connection != null)
                future.addListener(f -> handleMessageFuture(f, msg, connection));
            wroteOnce = true;
        }

        // as this is an infrequent operation, don't bother coordinating with the instance-level flush task
        if (wroteOnce)
            channel.flush();
    }

    void close()
    {
        if (closed)
            return;

        closed = true;
        channel.close();
    }

    /**
     * Close the underlying channel but only after having make sure every pending message has been properly sent.
     */
    void softClose()
    {
        if (closed)
            return;

        closed = true;
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    protected abstract ChannelFuture write(QueuedMessage message);

    abstract void onMessageProcessed(ChannelHandlerContext ctx);
    abstract void onTriggeredFlush(ChannelHandlerContext ctx);

    /**
     * Handles the no-coalescing case.
     */
    private static class SimpleOutChannel extends OutChannel
    {
        private SimpleOutChannel(Channel channel)
        {
            super(channel);
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            pendingMessageCount.incrementAndGet();
            // We don't truly want to flush on every message but we do want to wake-up the netty event loop for the
            // channel so the message is processed right away, which is why we use writeAndFlush. This won't actually
            // flushed though because onTriggeredFlush, which MessageOutHandler delegates to, does nothing. We will
            // flush after the message is processed though if there is no pending one due to onMessageProcessed.
            // See the class javadoc for context and much more details.
            return channel.writeAndFlush(message);
        }

        void onMessageProcessed(ChannelHandlerContext ctx)
        {
            if (pendingMessageCount.decrementAndGet() == 0)
                ctx.flush();
        }

        void onTriggeredFlush(ChannelHandlerContext ctx)
        {
            // Don't actually flush on "normal" flush calls to the channel.
        }
    }

    /**
     * Handles the coalescing case.
     */
    private static class CoalescingOutChannel extends OutChannel
    {
        private static final int MIN_MESSAGES_FOR_COALESCE = DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages();

        private final CoalescingStrategy strategy;
        private final AtomicBoolean scheduledFlush = new AtomicBoolean(false);

        private CoalescingOutChannel(Channel channel, CoalescingStrategy strategy)
        {
            super(channel);
            this.strategy = strategy;
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            long pendingCount = pendingMessageCount.incrementAndGet();
            ChannelFuture future = channel.write(message);

            strategy.newArrival(message);

            // if we lost the race to set the state, simply write to the channel (no flush)
            if (!scheduledFlush.compareAndSet(false, true))
                return future;

            long flushDelayNanos;
            // if we've hit the minimum number of messages for coalescing or we've run out of coalesce time, flush.
            // note: we check the exact count, instead of greater than or equal to, of message here to prevent a flush task
            // for each message. There will be, of course, races with the consumer decrementing the pending counter,
            // but that's still less excessive flushes.
            if (pendingCount == MIN_MESSAGES_FOR_COALESCE || (flushDelayNanos = strategy.currentCoalescingTimeNanos()) <= 0)
            {
                scheduledFlush.set(false);
                channel.flush();
            }
            else
            {
                // calling schedule() on the eventLoop will force it to wake up (if not already executing) and schedule the task
                channel.eventLoop().schedule(() -> {
                    scheduledFlush.set(false);
                    // we then execute() the flush() like this because netty runs the scheduled tasks before consuming from
                    // it's queue, which means it would process the flush() before any of the write tasks if we had just called
                    // flush() directly. by submitting via eventLoop().execute(), we ensure the flush task is enqueued
                    // at the end (after the write tasks).
                    channel.eventLoop().execute(channel::flush);
                }, flushDelayNanos, TimeUnit.NANOSECONDS);
            }
            return future;
        }

        void onMessageProcessed(ChannelHandlerContext ctx)
        {
            pendingMessageCount.decrementAndGet();
        }

        void onTriggeredFlush(ChannelHandlerContext ctx)
        {
            // When coalescing, obey the flush calls normally
            ctx.flush();
        }
    }
}
