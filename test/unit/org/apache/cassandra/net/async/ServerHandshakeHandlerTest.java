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
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ServerHandshakeHandler.State;

public class ServerHandshakeHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    static final String SHH_HANDLER_NAME = "ServerHandshakeHandler#0";
    private static final Consumer<MessageInWrapper> NOP_CONSUMER = msg -> {};

    private ByteBuf buf;

    @BeforeClass
    public static void beforeClass()
    {
        ServerHandshakeHandler.handshakeHandlerChannelHandlerName = SHH_HANDLER_NAME;
    }

    @After
    public void tearDown()
    {
        if (buf != null)
            buf.release();
    }

    @Test
    public void handleAuthenticate_Good()
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(true));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        boolean result = handler.handleAuthenticate(addr, channel.pipeline().firstContext());
        Assert.assertTrue(result);
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void handleAuthenticate_Bad()
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        boolean result = handler.handleAuthenticate(addr, channel.pipeline().firstContext());
        Assert.assertFalse(result);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleStart_NotEnoughInputBytes() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ByteBuf buf = Unpooled.EMPTY_BUFFER;
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.START, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test (expected = IOException.class)
    public void handleStart_BadMagic() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC << 2);
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
    }

    @Test
    public void handleStart_VersionTooHigh() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.eventLoop();
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(ClientHandshakeHandler.createHeader(MESSAGING_VERSION + 1, true, NettyFactory.Mode.MESSAGING));
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleStart_VersionLessThan2_0() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(ClientHandshakeHandler.createHeader(MessagingService.VERSION_12, true, NettyFactory.Mode.MESSAGING));
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleStart_HappyPath_Messaging() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(ClientHandshakeHandler.createHeader(MESSAGING_VERSION, true, NettyFactory.Mode.MESSAGING));
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.AWAIT_MESSAGING_START_RESPONSE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
    }

    @Test
    public void handleMessagingStartResponse_NotEnoughInputBytes() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ByteBuf buf = Unpooled.EMPTY_BUFFER;
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.AWAIT_MESSAGING_START_RESPONSE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void handleMessagingStartResponse_BadMaxVersion() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MESSAGING_VERSION + 1);
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleMessagingStartResponse_HappyPath() throws IOException
    {
        ServerHandshakeHandler handler = new ServerHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MESSAGING_VERSION);
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.MESSAGING_HANDSHAKE_COMPLETE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(new TestAuthenticator(false)));
        ChannelPipeline pipeline = channel.pipeline();
        ServerHandshakeHandler.setupMessagingPipeline(pipeline, ServerHandshakeHandler.createHandlers(addr.getAddress(), false, MESSAGING_VERSION, NOP_CONSUMER));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(new TestAuthenticator(false)));
        ChannelPipeline pipeline = channel.pipeline();
        ServerHandshakeHandler.setupMessagingPipeline(pipeline, ServerHandshakeHandler.createHandlers(addr.getAddress(), true, MESSAGING_VERSION, NOP_CONSUMER));
        Assert.assertNotNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
    }

    @Test
    public void setupPipeline_WithModernMessageHandler()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(new TestAuthenticator(false)));
        ChannelPipeline pipeline = channel.pipeline();
        ServerHandshakeHandler.setupMessagingPipeline(pipeline, ServerHandshakeHandler.createHandlers(addr.getAddress(), true, MESSAGING_VERSION, NOP_CONSUMER));
        Assert.assertNotNull(pipeline.get(MessageInHandler.class));
        Assert.assertNull(pipeline.get(LegacyClientHandler.class));
    }

    @Test
    public void setupPipeline_WithLegacyMessageHandler()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(new TestAuthenticator(false)));
        ChannelPipeline pipeline = channel.pipeline();
        ServerHandshakeHandler.setupMessagingPipeline(pipeline, ServerHandshakeHandler.createHandlers(addr.getAddress(), true, MessagingService.VERSION_12, NOP_CONSUMER));
        Assert.assertNull(pipeline.get(MessageInHandler.class));
        Assert.assertNotNull(pipeline.get(LegacyClientHandler.class));
    }
}
