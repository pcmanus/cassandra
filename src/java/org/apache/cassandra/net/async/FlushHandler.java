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
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Flushes messages at appropriate time.
 * <p>
 * When we flush depends on whether message coalescing is enabled or not.
 * <p>
 * If coalescing isn't enabled, we simply flush on every message.
 * <p>
 * If coalescing is enabled, how often we flush is delegated to the coalescing strategy. More precisely, when a new
 * message arrives:
 * <ul>
 *   <li>either a flush has been scheduled (by a previous message) already, and we just wait on that</li>
 *   <li>or no flush is scheduled and we ask the coalescing strategy when to schedule a flush</li>
 * </ul>
 * <p>
 * There is an obvious race between a new message being handled with a flush being already scheduled, and that flush
 * being executed: we want to avoid having the flush actually execute before the new message is written, but no flush
 * being scheduled for that message. To avoid that, we make sure a schedule flush deregister itself before executing,
 * and when we handle a new message, we write it first, and check if there is a registered flush second. Hence a new
 * message always has a flush executed after it is written: either because we see a flush registered _after_ the write,
 * and so we know that flush will also execute _after_ said write, or because we don't see a registered flush and
 * schedule one (based on the coalescing strategy).
 */
class FlushHandler extends ChannelOutboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(FlushHandler.class);

    private final CoalescingStrategy coalescingStrategy;

    // The currently scheduled flush, or null.
    private volatile ScheduledFuture<?> scheduledFlush;

    private volatile boolean closed;

    FlushHandler(CoalescingStrategy coalescingStrategy)
    {
        this.coalescingStrategy = coalescingStrategy;
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
        ctx.write(msg, promise);

        if (!coalescingStrategy.isCoalescing())
        {
            assert scheduledFlush == null;
            ctx.flush();
            return;
        }

        coalescingStrategy.newArrival(queuedMessage);

        // If there is a registered flush, it hasn't executed (as of the test below, since scheduled flushes
        // deregister themself before execution), so it will flush our prior write and we're done.
        if (scheduledFlush != null)
            return;

        long flushDelayNanos = coalescingStrategy.currentCoalescingTimeNanos();
        if (flushDelayNanos <= 0)
        {
            ctx.flush();
            return;
        }

        scheduledFlush = ctx.executor().schedule(() -> {
            // It's important to deregister first, see above
            scheduledFlush = null;
            ctx.flush();
        }, flushDelayNanos, TimeUnit.NANOSECONDS);
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
        // Grabs reference to make sure we don't race with the task execution (which sets scheduledFlush to null)
        ScheduledFuture<?> registeredFlush = scheduledFlush;
        if (registeredFlush != null)
        {
            registeredFlush.cancel(false);
            scheduledFlush = null;
        }

        ctx.close(promise);
    }
}
