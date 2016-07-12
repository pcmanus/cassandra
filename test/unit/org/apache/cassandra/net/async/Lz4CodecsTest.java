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

import com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class Lz4CodecsTest
{
    /** small enough to not be compressed */
    private static final String SHORT_SAMPLE = "hello, world!";

    /** large enough to be compressed. taken from James Joyce's "Ulysseus", http://www.gutenberg.org/files/4300/4300-h/4300-h.htm */
    private static final String LONGER_SAMPLE = "Stately, plump Buck Mulligan came from the stairhead, bearing a bowl of lather on which a mirror and a razor lay crossed. A yellow dressinggown, ungirdled, was sustained gently behind him on the mild morning air. He held the bowl aloft and intoned, ";

    private EmbeddedChannel embeddedChannel;
    private ByteBuf buf;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel(new Lz4FrameEncoder(true), new Lz4FrameDecoder(true));
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();

        // finish() calls close() on the codecs, and LZ4FRameEncoder sends a last message on close.
        // so there will be a message in the outbound part of the channel (this is ok!)
        Assert.assertTrue(embeddedChannel.finishAndReleaseAll());
    }

    @Test
    public void roundTrip_ShortSample() throws Exception
    {
        roundTrip(SHORT_SAMPLE);
    }

    @Test
    public void roundTrip_LongerSample() throws Exception
    {
        roundTrip(LONGER_SAMPLE);
    }

    private void roundTrip(String s) throws Exception
    {
        buf = Unpooled.buffer(1024);
        buf.writeBytes(s.getBytes(Charsets.UTF_8));

        Assert.assertTrue(embeddedChannel.writeOutbound(buf));
        Assert.assertFalse(embeddedChannel.outboundMessages().isEmpty());

        Assert.assertTrue(embeddedChannel.writeInbound(embeddedChannel.readOutbound()));
        Assert.assertFalse(embeddedChannel.inboundMessages().isEmpty());

        ByteBuf uncompressed = (ByteBuf) embeddedChannel.readInbound();
        byte[] b = new byte[uncompressed.readableBytes()];
        uncompressed.getBytes(uncompressed.readerIndex(), b);
        Assert.assertEquals(s, new String(b, Charsets.UTF_8));
        uncompressed.release();
        embeddedChannel.releaseInbound();
        embeddedChannel.releaseOutbound();
    }
}
