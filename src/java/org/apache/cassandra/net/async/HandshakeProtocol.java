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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;

/**
 * Messages for the handshake phase of the inter-node protocol.
 * <p>
 * The handshake main purpose is to establish a version that both side can talk, as well as exchanging a few connection
 * options/parameters. It is composed of 3 messages, the first being sent by the initiator of the connection. The other
 * side then answer with the 2nd message. At that point, if a version mismatch is detected by the connection initiator,
 * it will simply disconnect to reconnect with a more appropriate version. But the version is acceptable, the connection
 * initiator sends the third message of the protocol, after which it considers the connection ready.
 * <p>
 * See below for a more precise description of each of those 3 messages.
 * <p>
 * Note that this handshake protocol doesn't fully apply to streaming. For streaming, only the first message is send,
 * after which the streaming protocol takes over (not documented here)
 */
class HandshakeProtocol
{
    /**
     * The initial message send when a node creates a new connection to a remote peer. This message contains:
     *   1) the {@link MessagingService#PROTOCOL_MAGIC} number (4 bytes).
     *   2) the connection "header" (4 bytes), which encodes:
     *      - the version the initiator thinks should be used for the connection (in practice, either the initiator
     *        version if it's the first time we connect to that remote since startup, or the last version known for that
     *        peer otherwise).
     *      - the "mode" of the connection: whether it is for streaming or for messaging.
     *      - whether compression should be used or not (if it is, compression is enabled _after_ the last message of the
     *        handshake has been sent).
     * <p>
     * More precisely, the message is specified by:
     * <pre>
     * {@code
     *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |U U C         S|                |                               |
     * |N N M         T|     VERSION    |             unused            |
     * |U U P         R|                |                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     * UNU - unused bits lowest two bits; from a historical note: used to be "serializer type," which was always Binary
     * CMP - compression enabled bit
     * STR - flag to indicate if the connection is for streaming; if not, it is for inter-node messaging
     * VERSION - if a streaming connection, indicates the streaming protocol version {@link org.apache.cassandra.streaming.messages.StreamMessage#CURRENT_VERSION};
     * if a messaging connection, indicates the messaging protocol version the initiator *thinks* should be used.
     */
    static class FirstHandshakeMessage
    {
        /** Contains the PROTOCOL_MAGIC (int) and "header" (int). */
        private static final int LENGTH = 8;

        final int messagingVersion;
        final NettyFactory.Mode mode;
        final boolean compressionEnabled;

        FirstHandshakeMessage(int messagingVersion, NettyFactory.Mode mode, boolean compressionEnabled)
        {
            this.messagingVersion = messagingVersion;
            this.mode = mode;
            this.compressionEnabled = compressionEnabled;
        }

        @VisibleForTesting
        int encodeHeader()
        {
            int header = 0;
            if (compressionEnabled)
                header |= 1 << 2;
            if (mode == NettyFactory.Mode.STREAMING)
                header |= 8;

            header |= (messagingVersion << 8);
            return header;
        }

        public ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.buffer(LENGTH);
            buffer.writeInt(MessagingService.PROTOCOL_MAGIC);
            buffer.writeInt(encodeHeader());
            return buffer;
        }

        static FirstHandshakeMessage maybeDecode(ByteBuf in) throws IOException
        {
            if (in.readableBytes() < LENGTH)
                return null;

            MessagingService.validateMagic(in.readInt());
            int header = in.readInt();
            int version = MessagingService.getBits(header, 15, 8);
            NettyFactory.Mode mode = MessagingService.getBits(header, 3, 1) == 1
                                     ? NettyFactory.Mode.STREAMING
                                     : NettyFactory.Mode.MESSAGING;
            boolean compressed = MessagingService.getBits(header, 2, 1) == 1;
            return new FirstHandshakeMessage(version, mode, compressed);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof FirstHandshakeMessage))
                return false;

            FirstHandshakeMessage that = (FirstHandshakeMessage)other;
            return this.messagingVersion == that.messagingVersion
                   && this.mode == that.mode
                   && this.compressionEnabled == that.compressionEnabled;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(messagingVersion, mode, compressionEnabled);
        }
    }

    /**
     * The second message of the handshake, sent by the node receiving the first message back to the connection
     * initiator. This message contains the 4 bytes actual messaging version of the peer sending that 2nd message
     * (so {@link org.apache.cassandra.net.MessagingService#current_version}).
     */
    static class SecondHandshakeMessage
    {
        /** The messaging version sent by the receiving peer (int). */
        private static final int LENGTH = 4;

        final int messagingVersion;

        SecondHandshakeMessage(int messagingVersion)
        {
            this.messagingVersion = messagingVersion;
        }

        public ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.buffer(LENGTH);
            buffer.writeInt(messagingVersion);
            return buffer;
        }

        static SecondHandshakeMessage maybeDecode(ByteBuf in)
        {
            return in.readableBytes() >= LENGTH ? new SecondHandshakeMessage(in.readInt()) : null;
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof SecondHandshakeMessage
                   && this.messagingVersion == ((SecondHandshakeMessage) other).messagingVersion;
        }

        @Override
        public int hashCode()
        {
            return Integer.hashCode(messagingVersion);
        }
    }

    /**
     * The third message of the handshake, sent by the connection initiator on reception of the 2nd message. This
     * message contains:
     *   1) the connection initiator messaging version (4 bytes) - {@link org.apache.cassandra.net.MessagingService#current_version}.
     *   2) the connection initiator broadcast address as encoded by {@link org.apache.cassandra.net.CompactEndpointSerializationHelper}.
     *      This can be either 5 bytes for an IPv4 address, or 17 bytes for an IPv6 one.
     * <p>
     * This message concludes the handshake protocol. After that, the connection will used either for streaming, or to
     * send messages. If the connection is to be compressed, compression is enabled only after this message is sent/received.
     */
    static class ThirdHandshakeMessage
    {
        /**
         * The third message contains the version and IP address of the sending node. Because the IP can be either IPv4 or
         * IPv6, this can be either 9 (4 for version + 5 for IP) or 21 (4 for version + 17 for IP) bytes. Since we can't know
         * a priori if the IP address will be v4 or v6, go with the minimum required bytes and hope that if the address is
         * v6, we'll have the extra 12 bytes in the packet.
         */
        private static final int MIN_LENGTH = 9;

        final int messagingVersion;
        final InetAddress address;

        ThirdHandshakeMessage(int messagingVersion, InetAddress address)
        {
            this.messagingVersion = messagingVersion;
            this.address = address;
        }

        public ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.buffer(Integer.BYTES + CompactEndpointSerializationHelper.serializedSize(address));
            buffer.writeInt(messagingVersion);
            try
            {

                @SuppressWarnings("resource")
                DataOutput bbos = new ByteBufOutputStream(buffer);
                CompactEndpointSerializationHelper.serialize(address, bbos);
                return buffer;
            }
            catch (IOException e)
            {
                // Shouldn't happen, we're serializing in memory.
                throw new AssertionError(e);
            }
        }

        static ThirdHandshakeMessage maybeDecode(ByteBuf in) throws IOException
        {
            if (in.readableBytes() < MIN_LENGTH)
                return null;

            int version = in.readInt();
            @SuppressWarnings("resource")
            DataInput inputStream = new ByteBufInputStream(in);
            InetAddress address = CompactEndpointSerializationHelper.deserialize(inputStream);
            return new ThirdHandshakeMessage(version, address);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ThirdHandshakeMessage))
                return false;

            ThirdHandshakeMessage that = (ThirdHandshakeMessage)other;
            return this.messagingVersion == that.messagingVersion
                   && Objects.equals(this.address, that.address);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(messagingVersion, address);
        }
    }
}
