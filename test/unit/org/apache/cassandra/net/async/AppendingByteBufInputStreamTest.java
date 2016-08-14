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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.io.util.DataInputBuffer;

public class AppendingByteBufInputStreamTest
{
    private AppendingByteBufInputStream inputStream;

    @Before
    public void setUp()
    {
        inputStream = new AppendingByteBufInputStream();
    }

    @After
    public void tearDown()
    {
        inputStream.close();
    }

    @Test
    public void read_EmptiedCurrentBuffer() throws IOException
    {
        ByteBuf buf1 = Unpooled.buffer(4, 4);
        buf1.writerIndex(3);
        buf1.readerIndex(3);
        Assert.assertEquals(1, buf1.refCnt());
        inputStream.append(buf1);

        ByteBuf buf2 = Unpooled.buffer(8, 8);
        Assert.assertEquals(1, buf2.refCnt());
        int val = 42;
        buf2.writeByte(val);
        inputStream.append(buf2);

        Assert.assertEquals(val, inputStream.read());
        Assert.assertEquals(0, buf1.refCnt());
        Assert.assertEquals(1, buf2.refCnt());
    }

    @Test
    public void readByte_FirstInvocation() throws IOException
    {
        ByteBuf buf = Unpooled.buffer(8, 8);
        int val = 42;
        buf.writeByte(val);
        inputStream.append(buf);
        Assert.assertEquals(val, inputStream.read());
    }

    @Test
    public void readLong_FirstInvocation() throws IOException
    {
        ByteBuf buf = Unpooled.buffer(8, 8);
        long val = 4227462934L;
        buf.writeLong(val);
        inputStream.append(buf);
        assertLongInStream(val);
    }

    private void assertLongInStream(long val) throws IOException
    {
        byte[] ret = new byte[8];
        Assert.assertEquals(8, inputStream.read(ret));
        long l = new DataInputBuffer(ret).readLong();
        Assert.assertEquals(val, l);
    }

    @Test
    public void readLong_TwoBuffers() throws IOException
    {
        ByteBuf empty = Unpooled.buffer(4, 4);
        empty.writeInt(0);
        inputStream.append(empty); // will be released internally
        ByteBuf buf = Unpooled.buffer(4, 4);
        int val = 42;
        buf.writeInt(val);
        inputStream.append(buf);
        assertLongInStream(val);
        Assert.assertEquals(0, empty.refCnt());
    }

    @Test
    public void close()
    {
        ByteBuf buf1 = Unpooled.buffer(8, 8);
        ByteBuf buf2 = Unpooled.buffer(8, 8);
        Assert.assertEquals(1, buf1.refCnt());
        Assert.assertEquals(1, buf2.refCnt());

        inputStream.append(buf1);
        inputStream.append(buf2);
        inputStream.close();

        Assert.assertEquals(0, buf1.refCnt());
        Assert.assertEquals(0, buf2.refCnt());
    }}
