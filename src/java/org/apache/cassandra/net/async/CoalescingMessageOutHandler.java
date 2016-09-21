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

import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.PendingWriteQueue;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Coalesces messages before sending them to a peer. In {@link CoalescingStrategies}, after each message, or group of messages,
 * is pulled off the queue (via {@link BlockingQueue#drainTo(Collection)}), and can be logged for debugging purposes
 * (see {@link CoalescingStrategy#debugTimestamp(long)}). We mimic that behavior, albeit in a non-blocking manner, using
 * {@link RateLimiter#tryAcquire()} semantics as well as a callback specific to the {@link CoalescingStrategy}.
 */
class CoalescingMessageOutHandler extends ChannelOutboundHandlerAdapter implements Iterable<QueuedMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(CoalescingMessageOutHandler.class);

    private final CoalescingStrategy coalescingStrategy;

    /**
     * A callback for the {@link CoalescingStrategy} in use that is invoked after each message is sent.
     */
    private final Consumer<Long> coalesceCallback;

    private final AtomicLong droppedMessageCount;

    /**
     * A queue to buffer the messages that are being coalesced. Not marked as 'final' as the constructor for
     * {@link PendingWriteQueue} requires a {@link ChannelHandlerContext}, whic we won't have until {@link #handlerAdded(ChannelHandlerContext)}.
     */
    private PendingWriteQueue queue;

    /**
     * A {@link Future} for the execution of the current batch of coalesced messages.
     */
    private ScheduledFuture<?> coalesceFuture;

    private volatile boolean closed;

    CoalescingMessageOutHandler(CoalescingStrategy coalescingStrategy, AtomicLong droppedMessageCount)
    {
        this.coalescingStrategy = coalescingStrategy;
        this.droppedMessageCount = droppedMessageCount;
        coalesceCallback = coalescingStrategy.coalesceNonBlockingCallback();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        queue = new PendingWriteQueue(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        if (closed)
        {
            promise.setFailure(new ClosedChannelException());
            return;
        }
        if (!(msg instanceof QueuedMessage))
        {
            promise.setFailure(new UnsupportedMessageTypeException("msg must be an instancce of " + QueuedMessage.class.getSimpleName()));
            return;
        }

        QueuedMessage queuedMessage = (QueuedMessage)msg;
        promise.addListener(future -> handleMessageFuture(future, coalesceCallback, queuedMessage));

        if (!coalescingStrategy.isCoalescing())
        {
            ctx.writeAndFlush(msg, promise);
            return;
        }

        boolean empty = queue.isEmpty();
        queue.add(msg, promise);
        if (empty)
        {
            // TODO:JEB figure out this horseshit
            long sleepTime = coalescingStrategy.coalesceNonBlocking(queuedMessage.timestampNanos(), 1);
            if (sleepTime <= 0)
            {
                doCoalesce(ctx);
            }
            else
            {
                coalesceFuture = ctx.executor().schedule(() -> {
                    doCoalesce(ctx);
                    coalesceFuture = null;
                }, sleepTime, TimeUnit.NANOSECONDS);
            }
        }
    }

    private void handleMessageFuture(Future<? super Void> future, Consumer<Long> coalesceCallback, QueuedMessage msg)
    {
        if (future.isSuccess())
        {
            if (coalesceCallback != null)
                coalesceCallback.accept(msg.timestampNanos());
        }
    }

    /**
     * The function to be executed that sends out the coalesced messages. If the message has not timed out, it will
     * be written to the channel for downstream consumers.
     *
     */
    @VisibleForTesting
    int doCoalesce(ChannelHandlerContext ctx)
    {
        if (closed)
            return 0;

        int count = queue.size();
        if (count == 0)
            return 0;

        while (true)
        {
            Object o = queue.current();
            if (o == null)
                break;

            QueuedMessage msg = (QueuedMessage)o;
            ChannelPromise promise = queue.remove();
            if (!msg.isTimedOut())
            {
                ctx.writeAndFlush(msg, promise);
            }
            else
            {
                promise.cancel(false);
                droppedMessageCount.incrementAndGet();
            }
        }
        return count;
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception
    {
        doCoalesce(ctx);
        ctx.flush();
    }

    @VisibleForTesting
    void addToQueue(QueuedMessage msg, ChannelPromise promise)
    {
        queue.add(msg, promise);
    }

    /**
     * Disable sending of any coalesced messages.
     *
     * Note: will not be called on the netty IO thread.
     */
    void setClosed()
    {
        closed = true;
    }


    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        closed = true;

        if (coalesceFuture != null)
        {
            coalesceFuture.cancel(false);
            coalesceFuture = null;
        }
        if (queue != null)
        {
            ChannelPromise p;
            while ((p = queue.remove()) != null)
            {
                p.cancel(false);
            }
        }

        ctx.close(promise);
    }

    @Override
    public Iterator<QueuedMessage> iterator()
    {
        return new AbstractIterator<QueuedMessage>()
        {
            protected QueuedMessage computeNext()
            {
                Object o = queue.current();
                if (o == null)
                    return endOfData();
                queue.remove();
                // do *not* cancel the promise assocaited with the message as we don't want to trigger the callback
                return (QueuedMessage)o;
            }
        };
    }
}
