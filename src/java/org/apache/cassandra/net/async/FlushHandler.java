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

import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MessageSizeEstimator;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * This handler contains the story for flushing data to the socket. Unfortunetly, it's a complicated story.
 * <p>
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call,
 * copying the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data
 * (for some definition of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * There are some basic conditions under which we want to flush:
 * - we've filled up or exceeded the outbound buffer ({@link ChannelOutboundBuffer}) [1]
 * - there are no more messages in the queue left to process
 * <p>
 * One thing complicating this is how netty's event loop executes. It is woken up by external callers to the channel
 * invoking a flush (either {@link Channel#flush()} or one of the {@link Channel#writeAndFlush(Object)} methods).
 * (It is also woken up by it's own internal timeout on epoll_wait() system call.)
 * A plain {@link Channel#write(Object)} will only queue the message in the channel, and not wake up the event loop.
 * <p>
 * Another complicating piece is message coalescing {@link CoalescingStrategy}. The idea there is to (artificially)
 * delay the flushing of data in order to aggregate even more data before sending a group of packets out.
 *<p>
 * The trick is in the composition of these pieces, primarily when coalescing is enabled or disabled. In all cases,
 * we want to flush when the channel buffer is full.
 * <p>
 * Further there's a possible race of when the event loop wakes up on it's own (after epoll_wait timeout) vs. when a
 * coalescing needs to occur. If the event loop wakes up before a scheduled coalesce time, then we could get some unexpected/
 * uninteded flushing behavior. However, beacuse the coalesce strategy window times are (if configured sanely) dramatically
 * lower than the epoll_wait timeout, this "shouldn't" be a real-world problem (patches accepted if it is).
 *
 * <p>
 * [1] For those deperately interested, and only after you've read the entire class-level doc: You can register a custom
 * {@link MessageSizeEstimator} with a netty channel. When a message is written to the channel, it will check the
 * message size, and if the max ({@link ChannelOutboundBuffer}) size will be exceeded, a task to signal the "channel
 * writability changed" will be executed in the channel. That task, however, will wake up the event loop.
 * Thus if coalescing is enabled, the event loop will wake up prematurely and process (and flush!) the messages
 * currently in the queue, thus defeating an aspect of coalescing. Hence, we're not using that feature.
 */
class FlushHandler extends ChannelDuplexHandler
{
    private final AtomicLong pendingMessages;

    /**
     * Flag to indicate if messages on this channel are being coalesced. This informs how the flushing behavior
     * is actually executed; meainng, we different things if coalescing is enabled or not (see the class-level javadoc).
     */
    private final boolean isCoalescing;

    FlushHandler(OutboundConnectionParams updatedParams)
    {
        this.pendingMessages = updatedParams.pendingMessageCount;
        isCoalescing = updatedParams.coalesce;
    }

    /**
     * {@inheritDoc}
     *
     * If we are not coalescing, flush if there are no other more messages in the channel. If we are coalescing,
     * only flush at the {@link #flush(ChannelHandlerContext)} invocation.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    {
        ctx.write(msg);

        if (pendingMessages.decrementAndGet() == 0 && !isCoalescing)
            ctx.flush();
    }

    /**
     * {@inheritDoc}
     *
     * This method will be triggered when a producer thread writes a message the channel, and the size
     * of that message pushes the "open" count of bytes in the channel over the high water mark. The mechanics
     * of netty will wake up the event loop thread (if it's not already executing), trigger the
     * "channelWritabilityChanged" function, which be invoked *after* executing any pending write tasks in the netty queue.
     * Thus, when this method is invoked it's a great time to flush, to push all the written buffers to the kernel
     * for sending.
     *
     * Note: it doesn't matter if coalescing is enabled or disabled, once this function is invoked we want to flush
     * to free up memory.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    {
        if (!ctx.channel().isWritable())
            ctx.flush();

        ctx.fireChannelWritabilityChanged();
    }

    /**
     * {@inheritDoc}
     *
     * When coalese is enabled, we must respect this flush invocation. If coalesce is disabled, we only flush
     * on the conditions stated in the class-level documentation (no more messages in queue, outbound buffer size
     * over the configured limit).
     */
    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        if (isCoalescing)
            ctx.flush();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        ctx.flush();
        ctx.close(promise);
    }
}
