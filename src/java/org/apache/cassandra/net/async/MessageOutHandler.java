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
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 * <p>
 * On top of transforming {@link QueuedMessage} into bytes, this handler also feed back his progress to the linked
 * {@link OutChannel} so that the latter can take decision on when data should be flush (and this with and without
 * coalescing). See the javadoc on {@link OutChannel} for more details.
 */
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);
    private static final NoSpamLogger errorLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int MESSAGE_PREFIX_SIZE = 12;

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    private final OutChannel outChannel;

    MessageOutHandler(OutboundConnectionIdentifier connectionId, int targetMessagingVersion, OutChannel outChannel)
    {
        this.connectionId = connectionId;
        this.targetMessagingVersion = targetMessagingVersion;
        this.outChannel = outChannel;
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
                errorLogger.warn("{} illegal frame size: {}, ignoring message", connectionId, currentFrameSize);
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
            // Make sure we signal the outChanel even in case of errors.
            outChannel.onMessageProcessed(ctx);
        }
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
            promise.setFailure(new UnsupportedMessageTypeException(connectionId +
                                                                   " msg must be an instance of " + QueuedMessage.class.getSimpleName()));
        }
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
                                               msg.message.verb, connectionId.connectionAddress(), msg.message.serializedSize(targetMessagingVersion) + MESSAGE_PREFIX_SIZE);
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
            logger.warn("{} failed to capture the tracing info for an outbound message, ignoring", connectionId, e);
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
                         connectionId, out.capacity(), out.writerIndex(), msg.message);
    }

    /**
     * {@inheritDoc}
     *
     * This method will be triggered when a producer thread writes a message to the channel, and the size
     * of that message pushes the "open" count of bytes in the channel over the high water mark. The mechanics
     * of netty will wake up the event loop thread (if it's not already executing), trigger the
     * "channelWritabilityChanged" function, which is invoked *after* executing any pending write tasks in the netty queue.
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

    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        outChannel.onTriggeredFlush(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", connectionId, cause);
        else
            logger.warn("{} error", connectionId, cause);

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
