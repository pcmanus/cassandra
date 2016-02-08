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
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

/**
 * Parses out individual messages from the incoming buffers. Each message, both header and payload, is incrementally built up
 * from the available input data, then passed to the {@link #messageConsumer}.
 */
class MessageInHandler extends ChannelInboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

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

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    private final BiConsumer<MessageIn, Integer> messageConsumer;

    private State state;
    private MessageHeader messageHeader;

    MessageInHandler(InetAddress peer, int messagingVersion)
    {
        this (peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
    }

    MessageInHandler(InetAddress peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        this.messageConsumer = messageConsumer;
        state = State.READ_MAGIC;
    }

    /**
     * For each new message coming in, builds up a {@link MessageHeader} instance incrementatlly. This method
     * attempts to deserialize as much header information as it can out of the incoming {@link ByteBuf}, and
     * maintains a trivial state machine to remember progress across invocations.
     *
     * All exceptions are uncaught and thus percolate up to the netty channel, which will forward the exception down the pipeline
     * and will be caught by our last handler ({@link MessageInErrorHandler}), which closes the channel on error.
     */
    @SuppressWarnings("resource")
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        ByteBuf in = (ByteBuf)msg;
        DataInputPlus inputPlus = new DataInputPlus.DataInputStreamPlus(new ByteBufInputStream(in));

        // an imperfect optimization around calling in.readableBytes() all the time
        int readableBytes = in.readableBytes();

        switch (state)
        {
            case READ_MAGIC:
                if (readableBytes < 4)
                    return;
                int magic = in.readInt();
                MessagingService.validateMagic(magic);
                state = State.READ_MESSAGE_ID;
                readableBytes -= 4;
                // fall-through
            case READ_MESSAGE_ID:
                if (readableBytes < 4)
                    return;
                messageHeader = new MessageHeader();
                messageHeader.messageId = in.readInt();
                state = State.READ_TIMESTAMP;
                readableBytes -= 4;
                // fall-through
            case READ_TIMESTAMP:
                if (readableBytes < 4)
                    return;
                messageHeader.constructionTime = MessageIn.readConstructionTime(peer, inputPlus, System.currentTimeMillis());
                state = State.READ_IP_ADDRESS;
                readableBytes -= 4;
                // fall-through
            case READ_IP_ADDRESS:
                // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                // first, check that we can actually read the size byte, then check if we can read that number of bytes.
                // the "+ 1" is to make sure we have the size byte in addition to the serialized IP addr count of bytes in the buffer.
                int serializedAddrSize;
                if (readableBytes < 1 || readableBytes < (serializedAddrSize = in.getByte(in.readerIndex()) + 1))
                    return;
                messageHeader.from = CompactEndpointSerializationHelper.deserialize(inputPlus);
                state = State.READ_VERB;
                readableBytes -= serializedAddrSize;
                // fall-through
            case READ_VERB:
                if (readableBytes < 4)
                    return;
                messageHeader.verb = MessagingService.verbValues[in.readInt()];
                state = State.READ_PARAMETERS;
                readableBytes -= 4;
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
                readableBytes = in.readableBytes(); // we read an indeterminate number of bytes for the headers, so just ask the buffer again
                // fall-through
            case READ_PAYLOAD_SIZE:
                if (readableBytes < 4)
                    return;
                messageHeader.payloadSize = in.readInt();
                state = State.READ_PAYLOAD;
                readableBytes -= 4;
                // fall-through
            case READ_PAYLOAD:
                // TODO:JEB double check that the "payloadSize == 0" check is legit (for example, EchoMessage)
                if (messageHeader.payloadSize == 0 || readableBytes >= messageHeader.payloadSize)
                {
                    // TODO consider deserailizing the messge not on the IO thread
                    MessageIn<Object> messageIn = MessageIn.read(inputPlus, messagingVersion,
                                                                 messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                 messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);
                    if (messageIn != null)
                        messageConsumer.accept(messageIn, messageHeader.messageId);

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
        long constructionTime;
        InetAddress from;
        MessagingService.Verb verb;
        Map<String, byte[]> parameters;
        int payloadSize;
    }
}
