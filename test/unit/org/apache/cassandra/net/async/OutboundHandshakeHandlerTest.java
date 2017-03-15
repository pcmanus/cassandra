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
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;

public class OutboundHandshakeHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress localAddr = new InetSocketAddress("127.0.0.1", 0);
    private static final InetSocketAddress remoteAddr = new InetSocketAddress("127.0.0.2", 0);
    private static final String HANDLER_NAME = "clientHandshakeHandler";

    private EmbeddedChannel channel;
    private OutboundConnectionIdentifier connectionId;
    private OutboundHandshakeHandler handler;
    private HandshakeResult result;
    OutboundConnectionParams params;
    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        connectionId = OutboundConnectionIdentifier.small(localAddr, remoteAddr);
        params = OutboundConnectionParams.builder()
                                         .connectionId(connectionId)
                                         .coalescingStrategy(new FakeCoalescingStrategy(false))
                                         .callback(this::callbackHandler)
                                         .mode(NettyFactory.Mode.MESSAGING)
                                         .build();
        handler = new OutboundHandshakeHandler(params);
        channel.pipeline().addFirst(HANDLER_NAME, handler);
        result = null;
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
        Assert.assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void decode_SmallInput() throws Exception
    {
        buf = Unpooled.buffer(2, 2);
        List<Object> out = new LinkedList<>();
        handler.decode(channel.pipeline().firstContext(), buf, out);
        Assert.assertEquals(0, buf.readerIndex());
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void decode_SameMsgVersion() throws Exception
    {
        buf = Unpooled.buffer(4, 4);
        buf.writeInt(MESSAGING_VERSION);
        channel.writeInbound(buf);
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
        Assert.assertEquals(1, channel.outboundMessages().size());
        channel.releaseOutbound(); // throw away any responses from decode()

        Assert.assertEquals(MESSAGING_VERSION, result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.SUCCESS, result.outcome);
    }

    @Test
    public void decode_ReceivedLowerMsgVersion() throws Exception
    {
        buf = Unpooled.buffer(4, 4);
        int msgVersion = MESSAGING_VERSION - 1;
        buf.writeInt(msgVersion);
        channel.writeInbound(buf);
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
        Assert.assertTrue(channel.inboundMessages().isEmpty());

        Assert.assertEquals(msgVersion, result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.DISCONNECT, result.outcome);
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }

    @Test
    public void decode_ReceivedHigherMsgVersion() throws Exception
    {
        buf = Unpooled.buffer(4, 4);
        buf.writeInt(MESSAGING_VERSION);

        int msgVersion = MESSAGING_VERSION - 1;
        channel.pipeline().remove(HANDLER_NAME);
        handler = new OutboundHandshakeHandler(params, msgVersion);
        channel.pipeline().addFirst(HANDLER_NAME, handler);
        channel.writeInbound(buf);
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());

        Assert.assertEquals(MESSAGING_VERSION, result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.DISCONNECT, result.outcome);
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        EmbeddedChannel chan = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        ChannelPipeline pipeline =  chan.pipeline();
        params = OutboundConnectionParams.builder(params).compress(true).build();
        handler = new OutboundHandshakeHandler(params);
        pipeline.addFirst(handler);
        handler.setupPipeline(chan, MESSAGING_VERSION);
        Assert.assertNotNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        EmbeddedChannel chan = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        ChannelPipeline pipeline =  chan.pipeline();
        params = OutboundConnectionParams.builder(params).compress(false).build();
        handler = new OutboundHandshakeHandler(params);
        pipeline.addFirst(handler);
        handler.setupPipeline(chan, MESSAGING_VERSION);
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    private Void callbackHandler(HandshakeResult handshakeResult)
    {
        result = handshakeResult;
        return null;
    }
}
