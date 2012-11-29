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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ByteBuf utility methods.
 * Note that contrarily to ByteBufferUtil, these method do "read" the
 * ByteBuf advancing it's (read) position. They also write by
 * advancing the write position. Functions are also provided to create
 * ByteBuf while avoiding copies.
 */
public abstract class CBUtil
{
    private CBUtil() {}

    public static String readString(ByteBuf buf)
    {
        try
        {
            int length = buf.readUnsignedShort();
            return readString(buf, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by it's 2 bytes length");
        }
    }

    public static String readLongString(ByteBuf buf)
    {
        try
        {
            int length = buf.readInt();
            return readString(buf, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by it's 4 bytes length");
        }
    }

    private static String readString(ByteBuf buf, int length)
    {
        try
        {
            String str = buf.toString(buf.readerIndex(), length, CharsetUtil.UTF_8);
            buf.readerIndex(buf.readerIndex() + length);
            return str;
        }
        catch (IllegalStateException e)
        {
            // That's the way netty encapsulate a CCE
            if (e.getCause() instanceof CharacterCodingException)
                throw new ProtocolException("Cannot decode string as UTF8");
            else
                throw e;
        }
    }

    private static ByteBuf bytes(String str)
    {
        return Unpooled.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8));
    }

    public static ByteBuf shortToCB(int s)
    {
        ByteBuf buf = Unpooled.buffer(2);
        buf.writeShort(s);
        return buf;
    }

    public static ByteBuf intToCB(int i)
    {
        ByteBuf buf = Unpooled.buffer(4);
        buf.writeInt(i);
        return buf;
    }

    public static ByteBuf stringToCB(String str)
    {
        ByteBuf bytes = bytes(str);
        return Unpooled.wrappedBuffer(shortToCB(bytes.readableBytes()), bytes);
    }

    public static ByteBuf bytesToCB(byte[] bytes)
    {
        return Unpooled.wrappedBuffer(shortToCB(bytes.length), Unpooled.wrappedBuffer(bytes));
    }

    public static byte[] readBytes(ByteBuf buf)
    {
        try
        {
            int length = buf.readUnsignedShort();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read a byte array preceded by it's 2 bytes length");
        }
    }

    public static ByteBuf consistencyLevelToCB(ConsistencyLevel consistency)
    {
        return shortToCB(consistency.code);
    }

    public static ConsistencyLevel readConsistencyLevel(ByteBuf buf)
    {
        return ConsistencyLevel.fromCode(buf.readUnsignedShort());
    }

    public static <T extends Enum<T>> T readEnumValue(Class<T> enumType, ByteBuf buf)
    {
        String value = CBUtil.readString(buf);
        try
        {
            return Enum.valueOf(enumType, value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(String.format("Invalid value '%s' for %s", value, enumType.getSimpleName()));
        }
    }

    public static <T extends Enum<T>> ByteBuf enumValueToCB(T enumValue)
    {
        return stringToCB(enumValue.toString());
    }

    public static ByteBuf uuidToCB(UUID uuid)
    {
        return Unpooled.wrappedBuffer(UUIDGen.decompose(uuid));
    }

    public static UUID readUuid(ByteBuf buf)
    {
        byte[] bytes = new byte[16];
        buf.readBytes(bytes);
        return UUIDGen.getUUID(ByteBuffer.wrap(bytes));
    }

    public static ByteBuf longStringToCB(String str)
    {
        ByteBuf bytes = bytes(str);
        return Unpooled.wrappedBuffer(intToCB(bytes.readableBytes()), bytes);
    }

    public static List<String> readStringList(ByteBuf buf)
    {
        int length = buf.readUnsignedShort();
        List<String> l = new ArrayList<String>(length);
        for (int i = 0; i < length; i++)
            l.add(readString(buf));
        return l;
    }

    public static void writeStringList(ByteBuf buf, List<String> l)
    {
        buf.writeShort(l.size());
        for (String str : l)
            buf.writeBytes(stringToCB(str));
    }

    public static Map<String, String> readStringMap(ByteBuf buf)
    {
        int length = buf.readUnsignedShort();
        Map<String, String> m = new HashMap<String, String>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(buf).toUpperCase();
            String v = readString(buf);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringMap(ByteBuf buf, Map<String, String> m)
    {
        buf.writeShort(m.size());
        for (Map.Entry<String, String> entry : m.entrySet())
        {
            buf.writeBytes(stringToCB(entry.getKey()));
            buf.writeBytes(stringToCB(entry.getValue()));
        }
    }

    public static Map<String, List<String>> readStringToStringListMap(ByteBuf buf)
    {
        int length = buf.readUnsignedShort();
        Map<String, List<String>> m = new HashMap<String, List<String>>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(buf).toUpperCase();
            List<String> v = readStringList(buf);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringToStringListMap(ByteBuf buf, Map<String, List<String>> m)
    {
        buf.writeShort(m.size());
        for (Map.Entry<String, List<String>> entry : m.entrySet())
        {
            buf.writeBytes(stringToCB(entry.getKey()));
            writeStringList(buf, entry.getValue());
        }
    }

    public static ByteBuffer readValue(ByteBuf buf)
    {
        int length = buf.readInt();
        if (length < 0)
            return null;

        ByteBuf slice = buf.readSlice(length);
        if (slice.hasNioBuffer())
            return slice.nioBuffer();
        else
            return slice.copy().nioBuffer();
    }

    public static InetSocketAddress readInet(ByteBuf buf)
    {
        int addrSize = buf.readByte();
        byte[] address = new byte[addrSize];
        buf.readBytes(address);
        int port = buf.readInt();
        try
        {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        }
        catch (UnknownHostException e)
        {
            throw new ProtocolException(String.format("Invalid IP address (%d.%d.%d.%d) while deserializing inet address", address[0], address[1], address[2], address[3]));
        }
    }

    public static ByteBuf inetToCB(InetSocketAddress inet)
    {
        byte[] address = inet.getAddress().getAddress();
        ByteBuf buf = Unpooled.buffer(1 + address.length + 4);
        buf.writeByte(address.length);
        buf.writeBytes(address);
        buf.writeInt(inet.getPort());
        return buf;
    }

    public static class BufferBuilder
    {
        private final int size;
        private final ByteBuf[] buffers;
        private int i;

        public BufferBuilder(int simpleBuffers, int stringBuffers, int valueBuffers)
        {
            this.size = simpleBuffers + 2 * stringBuffers + 2 * valueBuffers;
            this.buffers = new ByteBuf[size];
        }

        public BufferBuilder add(ByteBuf buf)
        {
            buffers[i++] = buf;
            return this;
        }

        public BufferBuilder addString(String str)
        {
            ByteBuf bytes = bytes(str);
            add(shortToCB(bytes.readableBytes()));
            return add(bytes);
        }

        public BufferBuilder addValue(ByteBuffer bb)
        {
            add(intToCB(bb == null ? -1 : bb.remaining()));
            return add(bb == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(bb));
        }

        public ByteBuf build()
        {
            return Unpooled.wrappedBuffer(buffers);
        }
    }
}
