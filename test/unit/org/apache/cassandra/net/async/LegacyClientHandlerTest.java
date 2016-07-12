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
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.LegacyClientHandler.AppendingByteBufInputStream;

public class LegacyClientHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress ADDR = new InetSocketAddress("127.0.0.1", 9999);

    private final LinkedBlockingQueue<ByteBuf> queue = new LinkedBlockingQueue<>();
    private ByteBuf buf;

    @Before
    public void setUp()
    {
        queue.clear();
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void handler_channelRead_NotClosed()
    {
        buf = Unpooled.buffer(4, 4);
        Assert.assertEquals(0, queue.size());
        Assert.assertEquals(1, buf.refCnt());

        LegacyClientHandler handler = new LegacyClientHandler(ADDR.getAddress(), true, MESSAGING_VERSION, queue);
        handler.channelRead(null, buf);
        Assert.assertEquals(1, queue.size());
        Assert.assertEquals(1, buf.refCnt());
    }

    @Test
    public void handler_channelRead_Closed()
    {
        buf = Unpooled.buffer(4, 4);
        Assert.assertEquals(0, queue.size());
        Assert.assertEquals(1, buf.refCnt());

        LegacyClientHandler handler = new LegacyClientHandler(ADDR.getAddress(), true, MESSAGING_VERSION);
        handler.setClosed(true);
        handler.channelRead(null, buf);
        Assert.assertEquals(0, queue.size());
        Assert.assertEquals(0, buf.refCnt());
    }

    @Test
    public void inputStream_read_EmptyCurrentBuffer() throws IOException
    {
        ByteBuf buf1 = Unpooled.buffer(4, 4);
        buf1.writerIndex(3);
        buf1.readerIndex(3);

        ByteBuf buf2 = Unpooled.buffer(8, 8);
        Assert.assertEquals(1, buf1.refCnt());
        Assert.assertEquals(1, buf2.refCnt());
        int val = 42;
        buf2.writeByte(val);
        queue.add(buf2);

        try (AppendingByteBufInputStream inputStream = new AppendingByteBufInputStream(queue, buf1))
        {
            Assert.assertEquals(val, inputStream.read());
            Assert.assertEquals(0, buf1.refCnt());
            Assert.assertEquals(1, buf2.refCnt());
        }
        Assert.assertEquals(0, buf2.refCnt());
    }

    @Test
    public void inputStream_readByte_FirstInvocation() throws IOException
    {
        buf = Unpooled.buffer(8, 8);
        int val = 42;
        buf.writeByte(val);
        queue.add(buf);
        try (AppendingByteBufInputStream inputStream = new AppendingByteBufInputStream(queue))
        {
            Assert.assertEquals(val, inputStream.read());
        }
    }

    @Test
    public void inputStream_readLong_FirstInvocation() throws IOException
    {
        buf = Unpooled.buffer(8, 8);
        long val = 4227462934L;
        buf.writeLong(val);
        queue.add(buf);
        assertLongInStream(val);
    }

    private void assertLongInStream(long val) throws IOException
    {
        try (AppendingByteBufInputStream inputStream = new AppendingByteBufInputStream(queue))
        {
            byte[] ret = new byte[8];
            Assert.assertEquals(8, inputStream.read(ret));
            long l = new DataInputBuffer(ret).readLong();
            Assert.assertEquals(val, l);
        }
    }

    @Test
    public void inputStream_readLong_TwoBuffers() throws IOException
    {
        ByteBuf empty = Unpooled.buffer(4, 4);
        empty.writeInt(0);
        queue.add(empty); // will be released internally
        buf = Unpooled.buffer(4, 4);
        int val = 42;
        buf.writeInt(val);
        queue.add(buf);
        assertLongInStream(val);
    }

    @Test
    public void inputStream_close()
    {
        ByteBuf buf1 = Unpooled.buffer(8, 8);
        ByteBuf buf2;
        try (AppendingByteBufInputStream inputStream = new AppendingByteBufInputStream(queue, buf1))
        {
            buf2 = Unpooled.buffer(8, 8);
            queue.add(buf2);
            Assert.assertEquals(1, buf1.refCnt());
            Assert.assertEquals(1, buf2.refCnt());
        }
        Assert.assertEquals(0, buf1.refCnt());
        Assert.assertEquals(0, buf2.refCnt());
    }
}
