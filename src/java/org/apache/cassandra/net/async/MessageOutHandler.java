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
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.PromiseCombiner;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages. To optimize outbound buffers,
 * we pack as many messages into each buffer as we can; see {@link SwappingByteBufDataOutputStreamPlus} for details.
 */
class MessageOutHandler extends ChannelOutboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(CoalescingMessageOutHandler.class);

    private static final int MESSAGE_PREFIX_SIZE = 8;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    private final AtomicLong completedMessageCount;
    private final int bufferSize;

    private SwappingByteBufDataOutputStreamPlus dataOutputPlus;

    MessageOutHandler(int targetMessagingVersion, AtomicLong completedMessageCount, int bufferSize)
    {
        this.targetMessagingVersion = targetMessagingVersion;
        this.completedMessageCount = completedMessageCount;
        this.bufferSize = bufferSize;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        dataOutputPlus = new SwappingByteBufDataOutputStreamPlus(ctx, bufferSize);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object outboundMsg, ChannelPromise promise) throws Exception
    {
        QueuedMessage msg = (QueuedMessage)outboundMsg;
        captureTracingInfo(ctx, msg);
        serializeMessage(msg, promise);
        completedMessageCount.incrementAndGet();
    }

    /**
     * taken almost vertabim from {@link org.apache.cassandra.net.OutboundTcpConnection#writeConnected(QueuedMessage, boolean)}.
     * Code is copy/pasted from OTC to reduce the amount of changes made to it make merging/backporting easier.
     */
    private static void captureTracingInfo(ChannelHandlerContext ctx, QueuedMessage msg)
    {
        byte[] sessionBytes = msg.message.parameters.get(Tracing.TRACE_HEADER);
        if (sessionBytes != null)
        {
            UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
            TraceState state = Tracing.instance.get(sessionId);
            String message = String.format("Sending %s message to %s", msg.message.verb, ctx.channel().remoteAddress());
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

    /**
     * Frames and serializes a message to the {@code #dataOutputPlus}. If a message will span several outbound buffers,
     * the {@link #dataOutputPlus} will end up with several {@link ChannelPromise}s, one for each one of those buffers.
     * A {@link PromiseCombiner} is used to aggregate the results of those {@link ChannelPromise}s and link it to the message's
     * promise (the one that is a parameter to this method).
     */
    private void serializeMessage(QueuedMessage msg, ChannelPromise promise) throws IOException
    {
        dataOutputPlus.startMessage();
        // frame size does *not* include the magic and frame size int the value
        int currentFrameSize = MESSAGE_PREFIX_SIZE + msg.message.serializedSize(targetMessagingVersion);
        dataOutputPlus.writeInt(MessagingService.PROTOCOL_MAGIC);

        if (targetMessagingVersion >= MessagingService.VERSION_40)
            dataOutputPlus.writeInt(currentFrameSize);

        dataOutputPlus.writeInt(msg.id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        dataOutputPlus.writeInt((int) NanoTimeToCurrentTimeMillis.convert(msg.timestampNanos));
        msg.message.serialize(dataOutputPlus, targetMessagingVersion);

        PromiseCombiner promiseCombiner = new PromiseCombiner();
        for (ChannelPromise cp : dataOutputPlus.getPromises())
            promiseCombiner.add(cp);
        promiseCombiner.finish(promise);

        // next few lines are for debugging ... massively helpful!!
        int writeSize = dataOutputPlus.currentMessageByteCount - MESSAGE_PREFIX_SIZE;
        if (currentFrameSize != writeSize)
            logger.error("reported message size {}, actual message size {}, msg {}", currentFrameSize, writeSize, msg.message);

        dataOutputPlus.endMessage();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws IOException
    {
        if (dataOutputPlus != null)
            dataOutputPlus.flush();
        ctx.flush();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws IOException
    {
        // we could get here via two paths: normal close of the channel (due to app shutdown), or the socket closes or other error
        // In the latter case, don't bother to write/flush any further data ('cuz the socket is dead).
        if (ctx.channel().isActive())
        {
            flush(ctx);
        }

        dataOutputPlus.close();
        dataOutputPlus = null;
        ctx.close(promise);
    }

    /**
     * A {@link DataOutputStreamPlus} implementation that is backed by a {@link ByteBuf} that is swapped out with another when full.
     * Each buffer has a {@link ChannelPromise} associated with it (stored in a {@link WriteState} instance),
     * so the downstream progress of buffer (as it is flushed) can be observed. Each message will be serialized into one
     * or more buffers, and each of those buffer's promises are associated with the message. That way the message's promise
     * and be assocaited with the promises of the buffers into which it was serialized. Several messages may be packed into
     * the same buffer, so each of those messages will share the same promise.
     */
    private static class SwappingByteBufDataOutputStreamPlus extends UnbufferedDataOutputStreamPlus
    {
        /**
         * A reusable one-byte array so {@link #write(int)} doesn't need to instantiate one every time.
         */
        private final byte[] oneByte = new byte[1];

        private final ChannelHandlerContext ctx;
        private final int bufferCapacity;

        /**
         * A collection of {@link ChannelPromise}s that should be associated with the current {@link MessageOut}
         * that is being serialized. These promises will be matched up with the promise that was passed in at
         * {@link MessageOutHandler#write(ChannelHandlerContext, Object, ChannelPromise)}, so that promise can be properly
         * notified when all the buffers (and their own respective promises) have been handled downstream.
         */
        private final List<ChannelPromise> promises;

        /**
         * Captures the state related to the current open write buffer.
         * Also, opporutnitically set this to null, after {@link #flush()}, to avoid hanging on to unused memory.
         */
        private WriteState writeState;

        /**
         * Keeps track of the number of bytes written for the current message;
         */
        private int currentMessageByteCount;

        SwappingByteBufDataOutputStreamPlus(ChannelHandlerContext ctx, int bufferCapacity)
        {
            this.ctx = ctx;
            this.bufferCapacity = bufferCapacity;
            promises = new ArrayList<>(2);
        }

        /**
         * An indication the next message is about to be serialized, so setup the {@link #promises} array for it.
         */
        void startMessage()
        {
            promises.clear();
            if (writeState != null)
                promises.add(writeState.promise);
            currentMessageByteCount = 0;
        }

        @Override
        public void write(byte[] buffer, int offset, int count) throws IOException
        {
            int remaining = count;
            if (writeState == null)
                alloc();

            while (remaining > 0)
            {
                if (!writeState.buf.isWritable())
                    realloc();

                int currentBlockSize = Math.min(writeState.buf.writableBytes(), remaining);
                writeState.buf.writeBytes(buffer, offset, currentBlockSize);
                offset += currentBlockSize;
                remaining -= currentBlockSize;
            }
            currentMessageByteCount += count;
        }

        @Override
        public void write(int b) throws IOException
        {
            oneByte[0] = (byte)b;
            write(oneByte, 0, 1);
        }

        void alloc()
        {
            assert writeState == null : "write buf is not null";

            ByteBuf buf = ctx.alloc().directBuffer(bufferCapacity, bufferCapacity);
            ChannelPromise promise = ctx.newPromise();
            writeState = new WriteState(buf, promise);
            promises.add(promise);
        }

        void realloc()
        {
            assert writeState != null : "write buf is null";
            ctx.write(writeState.buf, writeState.promise);
            writeState = null;
            alloc();
        }

        @Override
        public void flush()
        {
            if (writeState != null)
            {
                ctx.writeAndFlush(writeState.buf, writeState.promise);
                writeState = null;
            }
        }

        @Override
        public void close()
        {
            if (ctx.channel().isActive())
            {
                flush();
            }
            else
            {
                writeState.buf.release();
                writeState.promise.setFailure(new ClosedChannelException());
            }
        }

        /**
         * Should be called when serializing a message is complete so resources can be freed up.
         */
        void endMessage()
        {
            promises.clear();
        }

        List<ChannelPromise> getPromises()
        {
            return promises;
        }

        /**
         * A simple struct to maintain the state of the current open buffer for writing and the promise associated with it.
         */
        private static final class WriteState
        {
            /**
             * The current {@link ByteBuf} that is being written to.
             */
            final ByteBuf buf;

            /**
             * A promise to be used with the current {@code buf} when calling
             * {@link ChannelHandlerContext#write(Object, ChannelPromise)}.
             */
            final ChannelPromise promise;

            private WriteState(ByteBuf buf, ChannelPromise promise)
            {
                this.buf = buf;
                this.promise = promise;
            }
        }
    }
}
