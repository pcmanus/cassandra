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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType;

public class OutboundHandshakeHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress localAddr = new InetSocketAddress("127.0.0.1", 0);
    private static final InetSocketAddress remoteAddr = new InetSocketAddress("127.0.0.2", 0);
    private static final String HANDLER_NAME = "clientHandshakeHandler";
    private static final ConnectionType CONNECTION_TYPE = ConnectionType.SMALL_MESSAGE;

    private EmbeddedChannel channel;
    private OutboundHandshakeHandler handler;
    private ConnectionHandshakeResult result;
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
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, remoteAddr, MESSAGING_VERSION,
                                                                       this::callbackHandler, null, NettyFactory.Mode.MESSAGING,
                                                                       false, false, new AtomicLong(), new AtomicLong(), new AtomicLong(), CONNECTION_TYPE);
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
    public void createHeader_FramedNoCompression()
    {
        int header = OutboundHandshakeHandler.createHeader(MESSAGING_VERSION, false, NettyFactory.Mode.MESSAGING);
        int version = MessagingService.getBits(header, 15, 8);
        Assert.assertEquals(MESSAGING_VERSION, version);
        boolean compressed = MessagingService.getBits(header, 2, 1) == 1;
        Assert.assertFalse(compressed);
    }

    @Test
    public void createHeader_FramedWithCompression()
    {
        int header = OutboundHandshakeHandler.createHeader(MESSAGING_VERSION, true, NettyFactory.Mode.MESSAGING);
        int version = MessagingService.getBits(header, 15, 8);
        Assert.assertEquals(MESSAGING_VERSION, version);
        boolean compressed = MessagingService.getBits(header, 2, 1) == 1;
        Assert.assertTrue(compressed);
    }

    @Test
    public void firstHandshakeMessage() throws IOException
    {
        buf = Unpooled.buffer(128, 128);
        int fakeHeader = 498234;
        OutboundHandshakeHandler.firstHandshakeMessage(buf, fakeHeader);
        MessagingService.validateMagic(buf.readInt());
        Assert.assertEquals(fakeHeader, buf.readInt());
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
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
        Assert.assertEquals(ConnectionHandshakeResult.Result.GOOD, result.result);
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
        Assert.assertEquals(ConnectionHandshakeResult.Result.DISCONNECT, result.result);
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
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, remoteAddr, msgVersion,
                                                                       this::callbackHandler, null, NettyFactory.Mode.MESSAGING,
                                                                       false, false, new AtomicLong(), new AtomicLong(), new AtomicLong(), CONNECTION_TYPE);
        handler = new OutboundHandshakeHandler(params);
        channel.pipeline().addFirst(HANDLER_NAME, handler);
        channel.writeInbound(buf);
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());

        Assert.assertEquals(MESSAGING_VERSION, result.negotiatedMessagingVersion);
        Assert.assertEquals(ConnectionHandshakeResult.Result.DISCONNECT, result.result);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        ChannelPipeline pipeline = new EmbeddedChannel(new ChannelOutboundHandlerAdapter()).pipeline();
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, remoteAddr, MESSAGING_VERSION,
                                                                       this::callbackHandler, null, NettyFactory.Mode.MESSAGING,
                                                                       true, false, new AtomicLong(), new AtomicLong(), new AtomicLong(), CONNECTION_TYPE);
        handler = new OutboundHandshakeHandler(params);
        handler.setupPipeline(pipeline, MESSAGING_VERSION);
        Assert.assertNotNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        ChannelPipeline pipeline = new EmbeddedChannel(new ChannelOutboundHandlerAdapter()).pipeline();
        OutboundConnectionParams params = new OutboundConnectionParams(localAddr, remoteAddr, MESSAGING_VERSION,
                                                                       this::callbackHandler, null, NettyFactory.Mode.MESSAGING,
                                                                       false, false, new AtomicLong(), new AtomicLong(), new AtomicLong(), CONNECTION_TYPE);
        handler = new OutboundHandshakeHandler(params);
        handler.setupPipeline(pipeline, MESSAGING_VERSION);
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    private Void callbackHandler(ConnectionHandshakeResult connectionHandshakeResult)
    {
        result = connectionHandshakeResult;
        return null;
    }
}
