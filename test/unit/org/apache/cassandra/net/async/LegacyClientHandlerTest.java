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

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.net.MessagingService;

public class LegacyClientHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress ADDR = new InetSocketAddress("127.0.0.1", 9999);

    private LegacyClientHandler handler;
    private AppendingByteBufInputStream inputStream;
    private ByteBuf buf;

    @Before
    public void setUp()
    {
        inputStream = new AppendingByteBufInputStream();
        handler = new LegacyClientHandler(ADDR.getAddress(), true, MESSAGING_VERSION, inputStream);
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void channelRead_NotClosed()
    {
        buf = Unpooled.buffer(4, 4);
        Assert.assertEquals(1, buf.refCnt());

        handler.channelRead(null, buf);
        Assert.assertEquals(1, inputStream.buffersInQueue());
        Assert.assertEquals(1, buf.refCnt());
    }

    @Test
    public void channelRead_Closed()
    {
        buf = Unpooled.buffer(4, 4);
        Assert.assertEquals(1, buf.refCnt());

        handler.setClosed(true);
        handler.channelRead(null, buf);
        Assert.assertEquals(0, inputStream.buffersInQueue());
        Assert.assertEquals(0, buf.refCnt());
    }

    @Test
    public void channelRead_NotByteBuffer()
    {
        handler.channelRead(null, "this is not legit");
        Assert.assertEquals(0, inputStream.buffersInQueue());
    }

    @Test
    public void channelRead_CloseInputStream()
    {
        buf = Unpooled.buffer(4, 4);
        Assert.assertEquals(1, buf.refCnt());
        inputStream.close();

        handler.channelRead(null, buf);
        Assert.assertEquals(0, inputStream.buffersInQueue());
        Assert.assertEquals(0, buf.refCnt());
    }
}
