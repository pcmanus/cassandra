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

import java.net.InetAddress;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.cassandra.db.monitoring.ConstructionTime;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

/**
 * A netty {@link ChannelHandler} responsible for deserializing internode messages. Assumes that the {@link ByteBuf}
 * parameter to {@link #decode(ChannelHandlerContext, ByteBuf, List)} is a buffer that holds a single message, due to
 * using {@link LengthFieldBasedFrameDecoder} in the pipeline before this handler.
 */
class MessageInHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    /**
     * The minimum number of bytes of a message's frame, which includes:
     * - int - protocol magic
     * - int - frame size
     */
    private static final int FRAME_HEADER_SIZE = 8;

    /**
     * The address of the node we are receiving messages from on this channel.
     */
    private final InetAddress peer;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    MessageInHandler(InetAddress peer, int targetMessagingVersion)
    {
        if (targetMessagingVersion < MessagingService.VERSION_40)
            throw new IllegalArgumentException(String.format("messaging version was %d, needs to be at least %d", targetMessagingVersion, MessagingService.VERSION_40));
        this.peer = peer;
        this.targetMessagingVersion = targetMessagingVersion;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        MessageInWrapper wrapper = readNextMessage(in);
        if (wrapper != null)
            out.add(wrapper);

        if (in.readableBytes() > 0)
            logger.warn("there are leftover bytes from parsing MessageIn, count = {}", in.readableBytes());
    }

    @VisibleForTesting
    @SuppressWarnings("resource")
    private MessageInWrapper readNextMessage(ByteBuf in) throws Exception
    {
        if (in.readableBytes() < FRAME_HEADER_SIZE)
            return null;
        in.markReaderIndex();
        int magic = in.readInt();
        MessagingService.validateMagic(magic);

        int messageSize = in.readInt();
        if (messageSize > in.readableBytes())
        {
            in.resetReaderIndex();
            return null;
        }

        // TODO if a message *really* large (think >= 100 MB) comes in, we're gonna blow out of memory just from buffering the damn thing up

        int id = in.readInt();
        DataInputPlus inputPlus = new DataInputStreamPlus(new ByteBufInputStream(in));
        ConstructionTime constructionTime = MessageIn.readTimestamp(peer, inputPlus, System.currentTimeMillis());

        // MessageIn.read() will return null if message or it's callback is expired, but it has still deserialized the message (skipped the bytes, actually),
        // so we don't need to worry about the stream/bytes/buffer/thingamabob being unconsumed.
        MessageIn message = MessageIn.read(inputPlus, targetMessagingVersion, id, constructionTime);

        return message != null ? new MessageInWrapper(message, id) : null;
    }
}
