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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MessageSizeEstimator;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages. This handler also contains the story
 * for flushing data to the socket. Unfortunetly, it's a complicated story.
 * <p>
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call,
 * copying the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data
 * (for some definition of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * There are some basic conditions under which we want to flush:
 * - we've filled up or exceeded the netty outbound buffer ({@link ChannelOutboundBuffer}) [1]
 * - there are no more messages in the queue left to process
 * <p>
 * One thing complicating this is how netty's event loop executes. It is woken up by external callers to the channel
 * invoking a flush (either {@link Channel#flush()} or one of the {@link Channel#writeAndFlush(Object)} methods).
 * (It is also woken up by it's own internal timeout on the epoll_wait() system call.)
 * A plain {@link Channel#write(Object)} will only queue the message in the channel, and not wake up the event loop.
 * <p>
 * Another complicating piece is message coalescing {@link CoalescingStrategy}. The idea there is to (artificially)
 * delay the flushing of data in order to aggregate even more data before sending a group of packets out.
 *<p>
 * The trick is in the composition of these pieces, primarily when coalescing is enabled or disabled. In all cases,
 * we want to flush when the netty outnound buffer is full or over capacity.
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
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);
    private static final NoSpamLogger errorLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int MESSAGE_PREFIX_SIZE = 12;

    private final InetSocketAddress remoteAddr;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    private final AtomicLong completedMessageCount;

    private final AtomicLong pendingMessageCount;

    private final AtomicLong droppedMessageCount;

    /**
     * Flag to indicate if messages on this channel are being coalesced. This informs how the flushing behavior
     * is actually executed; meaning, we do different things if coalescing is enabled or not (see the class-level javadoc).
     */
    private final boolean isCoalescing;

    private final ConnectionType connectionType;

    MessageOutHandler(OutboundConnectionParams params)
    {
        this (params.remoteAddr, params.protocolVersion, params.completedMessageCount,
              params.pendingMessageCount, params.coalesce, params.connectionType, params.droppedMessageCount);
    }

    MessageOutHandler(InetSocketAddress remoteAddr, int targetMessagingVersion, AtomicLong completedMessageCount,
                      AtomicLong pendingMessageCount, boolean isCoalescing, ConnectionType connectionType, AtomicLong droppedMessageCount)
    {
        this.remoteAddr = remoteAddr;
        this.targetMessagingVersion = targetMessagingVersion;
        this.completedMessageCount = completedMessageCount;
        this.pendingMessageCount = pendingMessageCount;
        this.isCoalescing = isCoalescing;
        this.connectionType = connectionType;
        this.droppedMessageCount = droppedMessageCount;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object o, ChannelPromise promise)
    {
        if (!isMessageValid(o, promise))
            return;

        ByteBuf out = null;
        try
        {
            QueuedMessage msg = (QueuedMessage) o;

            // frame size includes the magic and and other values *before* the actual serialized message.
            long currentFrameSize = MESSAGE_PREFIX_SIZE + msg.message.serializedSize(targetMessagingVersion);
            if (currentFrameSize > Integer.MAX_VALUE || currentFrameSize < 0)
            {
                errorLogger.warn("{} illegal frame size: {}, ignoring message", peerIdentifier(), currentFrameSize);
                return;
            }

            out = ctx.alloc().ioBuffer((int)currentFrameSize, (int)currentFrameSize);

            captureTracingInfo(msg);
            serializeMessage(msg, out);
            ctx.write(out, promise);
        }
        catch(Exception e)
        {
            if (out != null)
                out.release();
            exceptionCaught(ctx, e);
        }
        finally
        {
            // put the counter modifications here to make sure that, in the case of failure, we still update correctly
            completedMessageCount.incrementAndGet();
            // see class-level javadoc for details about flushing
            if (pendingMessageCount.decrementAndGet() == 0 && !isCoalescing)
                ctx.flush();
        }
    }

    /**
     * Centralized mechanism for identifying the peer and connection type to be used in error logging.
     */
    private String peerIdentifier()
    {
        return String.format("%s (%s)", remoteAddr.toString(), connectionType.toString() + ':');
    }

    /**
     * Test to see if the message passed in is a {@link QueuedMessage} and if it has timed out or not. If the checks fail,
     * this method has the side effect of modifying the {@link ChannelPromise}.
     */
    boolean isMessageValid(Object o, ChannelPromise promise)
    {
        // optimize for the common case
        if (o instanceof QueuedMessage)
        {
            if (!((QueuedMessage)o).isTimedOut())
            {
                return true;
            }
            else
            {
                promise.setFailure(ExpiredException.INSTANCE);
            }
        }
        else
        {
            promise.setFailure(new UnsupportedMessageTypeException(peerIdentifier() +
                                                                   " msg must be an instancce of " + QueuedMessage.class.getSimpleName()));
        }

        droppedMessageCount.incrementAndGet();
        return false;
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    private void captureTracingInfo(QueuedMessage msg)
    {
        try
        {
            byte[] sessionBytes = msg.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s, size = %d bytes",
                                               msg.message.verb, remoteAddr, msg.message.serializedSize(targetMessagingVersion) + MESSAGE_PREFIX_SIZE);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = msg.message.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    Tracing.instance.trace(ByteBuffer.wrap(sessionBytes), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (msg.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("{} failed to capture the tracing info for an outbound message, ignoring", peerIdentifier(), e);
        }
    }

    private void serializeMessage(QueuedMessage msg, ByteBuf out) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(msg.id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) NanoTimeToCurrentTimeMillis.convert(msg.timestampNanos));
        msg.message.serialize(new ByteBufDataOutputPlus(out), targetMessagingVersion);

        // next few lines are for debugging ... massively helpful!!
        // if we allocated too much buffer for this message, we'll log here.
        // if we allocated to little buffer space, we would have hit an exception when trying to write more bytes to it
        if (out.isWritable())
            errorLogger.error("{} reported message size {}, actual message size {}, msg {}",
                         peerIdentifier(), out.capacity(), out.writerIndex(), msg.message);
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
     * When message coalescing is enabled, we must respect this flush invocation. If coalesce is disabled, we only flush
     * on the conditions stated in the class-level documentation.
     */
    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        if (isCoalescing)
            ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", peerIdentifier(), cause);
        else
            logger.warn("{} error", peerIdentifier(), cause);

        ctx.fireExceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        ctx.flush();
        ctx.close(promise);
    }
}
