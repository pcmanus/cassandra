
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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.EnumSet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.TooLongFrameException;

import org.apache.cassandra.utils.ByteBufferUtil;

public class Frame
{
    public final Header header;
    public final ByteBuf body;
    public final Connection connection;

    /**
     * On-wire frame.
     * Frames are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  | stream  | opcode  |
     *   +---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ByteBuf body, Connection connection)
    {
        this.header = header;
        this.body = body;
        this.connection = connection;
    }

    public static Frame create(ByteBuf fullFrame, Connection connection)
    {
        assert fullFrame.readableBytes() >= Header.LENGTH : String.format("Frame too short (%d bytes)", fullFrame.readableBytes());

        int version = fullFrame.readByte();
        int flags = fullFrame.readByte();
        int streamId = fullFrame.readByte();
        int opcode = fullFrame.readByte();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        // version first byte is the "direction" of the frame (request or response)
        Message.Direction direction = Message.Direction.extractFromVersion(version);
        version = version & 0x7F;

        Header header = new Header(version, flags, streamId, Message.Type.fromOpcode(opcode, direction));
        return new Frame(header, fullFrame, connection);
    }

    public static Frame create(Message.Type type, int streamId, EnumSet<Header.Flag> flags, ByteBuf body, Connection connection)
    {
        Header header = new Header(Header.CURRENT_VERSION, flags, streamId, type);
        return new Frame(header, body, connection);
    }

    public static class Header
    {
        public static final int LENGTH = 8;
        public static final int CURRENT_VERSION = 1;

        public final int version;
        public final EnumSet<Flag> flags;
        public final int streamId;
        public final Message.Type type;

        private Header(int version, int flags, int streamId, Message.Type type)
        {
            this(version, Flag.deserialize(flags), streamId, type);
        }

        private Header(int version, EnumSet<Flag> flags, int streamId, Message.Type type)
        {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.type = type;
        }

        public static enum Flag
        {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING;

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < 8; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    public Frame with(ByteBuf newBody)
    {
        return new Frame(header, newBody, connection);
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder
    {
        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB
        private final Connection connection;

        public Decoder(Connection.Tracker tracker, Connection.Factory factory)
        {
            super(MAX_FRAME_LENTH, 4, 4, 0, 0, true);
            this.connection = factory.newConnection(tracker);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx)
        throws Exception
        {
            connection.registerChannel(ctx.channel());
        }

        @Override
        public Object decode(ChannelHandlerContext ctx, ByteBuf buffer)
        throws Exception
        {
            try
            {
                // We must at least validate that the frame version is something we support/know and it doesn't hurt to
                // check the opcode is not garbage. And we should do that indenpently of what is the the bytes corresponding
                // to the frame length are, i.e. we shouldn't wait for super.decode() to return non-null.
                if (buffer.readableBytes() == 0)
                    return null;

                int firstByte = buffer.getByte(0);
                Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
                int version = firstByte & 0x7F;
                // We really only support the current version so far
                if (version != Header.CURRENT_VERSION)
                    throw new ProtocolException("Invalid or unsupported protocol version: " + version);

                // Validate the opcode
                if (buffer.readableBytes() >= 4)
                    Message.Type.fromOpcode(buffer.getByte(3), direction);

                ByteBuf frame = (ByteBuf) super.decode(ctx, buffer);
                if (frame == null)
                {
                    return null;
                }
                return Frame.create(frame, connection);
            }
            catch (CorruptedFrameException e)
            {
                throw new ProtocolException(e.getMessage());
            }
            catch (TooLongFrameException e)
            {
                throw new ProtocolException(e.getMessage());
            }
        }
    }

    @Sharable
    public static class Encoder extends MessageToByteEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out)
        throws IOException
        {
            Message.Type type = frame.header.type;
            out.writeByte(type.direction.addToVersion(frame.header.version));
            out.writeByte(Header.Flag.serialize(frame.header.flags));
            out.writeByte(frame.header.streamId);
            out.writeByte(type.opcode);
            out.writeInt(frame.body.readableBytes());

            out.writeBytes(frame.body, frame.body.readerIndex(), frame.body.readableBytes());
        }
    }

    @Sharable
    public static class Decompressor extends MessageToMessageDecoder<Frame, Frame>
    {
        public Frame decode(ChannelHandlerContext ctx, Frame frame)
        throws IOException
        {
            if (!frame.header.flags.contains(Header.Flag.COMPRESSED))
                return frame;

            FrameCompressor compressor = frame.connection.getCompressor();
            if (compressor == null)
                return frame;

            return compressor.decompress(frame);
        }
    }

    @Sharable
    public static class Compressor extends MessageToMessageEncoder<Frame, Frame>
    {
        public Frame encode(ChannelHandlerContext ctx, Frame frame)
        throws IOException
        {
            // Never compress STARTUP messages
            if (frame.header.type == Message.Type.STARTUP || frame.connection == null)
                return frame;

            FrameCompressor compressor = frame.connection.getCompressor();
            if (compressor == null)
                return frame;

            frame.header.flags.add(Header.Flag.COMPRESSED);
            return compressor.compress(frame);

        }
    }
}
