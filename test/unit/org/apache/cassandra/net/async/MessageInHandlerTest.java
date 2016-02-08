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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;

public class MessageInHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.1", 0);
    private static final MessageOut msgOut = new MessageOut(REMOTE_ADDR.getAddress(), MessagingService.Verb.ECHO, null, null, Collections.<String, byte[]>emptyMap());

    private ByteBuf buf;

    @After
    public void tearDown()
    {
        if (buf != null)
            buf.release();
    }

    @Test
    public void decode_NotEnoughBytesToReadSerailizedMessage() throws Exception
    {
        MessageInHandler handler = new MessageInHandler(REMOTE_ADDR.getAddress(), MESSAGING_VERSION);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(128, 128);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(1);
        List<Object> out = new ArrayList<>(1);
        handler.decode(channel.pipeline().firstContext(), buf, out);
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void decode_ReadSerailizedMessage() throws Exception
    {
        MessageInHandler handler = new MessageInHandler(REMOTE_ADDR.getAddress(), MESSAGING_VERSION);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        buf = Unpooled.buffer(128, 128);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(frameSize(msgOut));
        buf.writeInt(1); // this is the id
        buf.writeInt((int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime()));
        msgOut.serialize(new ByteBufDataOutputPlus(buf), MESSAGING_VERSION);

        List<Object> out = new ArrayList<>(1);
        handler.decode(channel.pipeline().firstContext(), buf, out);
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
        Assert.assertEquals(1, out.size());
    }

    private int frameSize(MessageOut messageOut)
    {
        return 4 + 4 + messageOut.serializedSize(MESSAGING_VERSION);
    }

    private static class ByteBufDataOutputPlus extends ByteBufOutputStream implements DataOutputPlus
    {
        ByteBufDataOutputPlus(ByteBuf buffer)
        {
            super(buffer);
        }

        public void write(ByteBuffer buffer) throws IOException
        {
            buffer().writeBytes(buffer);
        }

        public void write(Memory memory, long offset, long length) throws IOException
        {
            buffer().writeBytes(memory.asByteBuffer(offset, (int)length));
        }

        public <R> R applyToChannel(Function<WritableByteChannel, R> c) throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
