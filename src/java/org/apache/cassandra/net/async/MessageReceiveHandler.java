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

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.db.monitoring.ConstructionTime;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

/**
 * Parses out individual messages from the incoming buffers. Each message, both header and payload, is incrementally built up
 * from the available input data, then sent on down through the netty pipeline.
 */
class MessageReceiveHandler extends ByteToMessageDecoder
{
    private enum State
    {
        READ_MAGIC,
        READ_MESSAGE_ID,
        READ_TIMESTAMP,
        READ_IP_ADDRESS,
        READ_VERB,
        READ_PARAMETERS,
        READ_PAYLOAD_SIZE,
        READ_PAYLOAD
    }

    private final InetAddress peer;
    private final int messagingVersion;

    private State state;
    private MessageHeader messageHeader;

    MessageReceiveHandler(InetAddress peer, int messagingVersion)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        state = State.READ_MAGIC;
    }

    /**
     * For each new message coming in, buils up a {@link MessageHeader} instance incrementatlly. This method
     * attempts to deserialize as much header information as it can out of the incoming {@link ByteBuf}, and
     * maintains a trivial state machine to remember progress across invocations.
     *
     * All exceptions are uncaught and this percolate up to {@link ByteToMessageDecoder#callDecode(ChannelHandlerContext, ByteBuf, List)}.
     * That method will send the exception down the pipeline and will be caught by our last handler, which closes the channel on error.
     */
    @SuppressWarnings("resource")
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        ByteBufInputStream byteBufInputStream = new ByteBufInputStream(in);
        DataInputPlus inputPlus = new DataInputPlus.DataInputStreamPlus(byteBufInputStream);

        switch (state)
        {
            case READ_MAGIC:
                if (in.readableBytes() < 4)
                    return;
                int magic = in.readInt();
                MessagingService.validateMagic(magic);
                state = State.READ_MESSAGE_ID;
                // fall-through
            case READ_MESSAGE_ID:
                if (in.readableBytes() < 4)
                    return;
                messageHeader = new MessageHeader();
                messageHeader.messageId = in.readInt();
                state = State.READ_TIMESTAMP;
                // fall-through
            case READ_TIMESTAMP:
                if (in.readableBytes() < 4)
                    return;
                messageHeader.constructionTime = MessageIn.readTimestamp(peer, inputPlus, System.currentTimeMillis());
                state = State.READ_IP_ADDRESS;
                // fall-through
            case READ_IP_ADDRESS:
                // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                // first check that we can actually read the size byte, then check if we can read that number of bytes
                if (in.readableBytes() < 1 || in.readableBytes() < in.getByte(in.readerIndex()))
                    return;
                messageHeader.from = CompactEndpointSerializationHelper.deserialize(inputPlus);
                state = State.READ_VERB;
                // fall-through
            case READ_VERB:
                if (in.readableBytes() < 4)
                    return;
                messageHeader.verb = MessagingService.verbValues[in.readInt()];
                state = State.READ_PARAMETERS;
                // fall-through
            case READ_PARAMETERS:
                // unfortunately, we do not have a size in bytes value here (only the number of parameters).
                // thus we must attempt to deserialize all the parameters and rely on an EOFException as indicator
                // of not enough bytes.
                in.markReaderIndex();
                try
                {
                    messageHeader.parameters = MessageIn.readParameters(inputPlus);
                }
                catch (EOFException eof)
                {
                    // EOF is ok, just means we ran out of bytes. reset the read index and wait for more bytes to come in
                    in.resetReaderIndex();
                    return;
                }
                state = State.READ_PAYLOAD_SIZE;
                // fall-through
            case READ_PAYLOAD_SIZE:
                if (in.readableBytes() < 4)
                    return;
                messageHeader.payloadSize = in.readInt();
                state = State.READ_PAYLOAD;
                // fall-through
            case READ_PAYLOAD:
                if (messageHeader.payloadSize == 0 || in.readableBytes() >= messageHeader.payloadSize)
                {
                    // TODO:JEB it's debatable if we want to deserailize the messge here on the IO thread (prolly should not)
                    MessageIn<Object> messageIn = MessageIn.read(new DataInputPlus.DataInputStreamPlus(new ByteBufInputStream(in)), messagingVersion,
                                                                 messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                 messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);
                    if (messageIn != null)
                        out.add(new MessageInWrapper(messageIn, messageHeader.messageId));

                    state = State.READ_MAGIC;
                    messageHeader = null;
                }
                break;
        }
    }

    // should ony be used for testing!!!
    @VisibleForTesting
    MessageHeader getMessageHeader()
    {
        return messageHeader;
    }

    /**
     * A simple struct to hold the message header data as it is being built up.
     */
    static class MessageHeader
    {
        int messageId;
        ConstructionTime constructionTime;
        InetAddress from;
        MessagingService.Verb verb;
        Map<String, byte[]> parameters;
        int payloadSize;
    }
}
