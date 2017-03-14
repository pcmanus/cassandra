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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class MessageOutHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private EmbeddedChannel channel;
    private MessageOutHandler handler;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        OutboundConnectionIdentifier connectionId = OutboundConnectionIdentifier.small(new InetSocketAddress("127.0.0.1", 0),
                                                                                       new InetSocketAddress("127.0.0.1", 0));
        channel = new EmbeddedChannel(handler);
        OutChannel outChan = OutChannel.create(channel, new FakeCoalescingStrategy(false));
        handler = new MessageOutHandler(connectionId, MESSAGING_VERSION, outChan);
    }

    @Test
    public void serializeMessage() throws IOException
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(1 <= channel.outboundMessages().size());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void wrongMessageType()
    {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid("this is the wrong message type", promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(UnsupportedMessageTypeException.class, promise.cause().getClass());
    }

    @Test
    public void unexpiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertTrue(handler.isMessageValid(msg, promise));

        // we won't know if it was successful yet, but we'll know if it's a failure because cause will be set
        Assert.assertNull(promise.cause());
    }

    @Test
    public void expiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1, 0, true, true);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid(msg, promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(ExpiredException.class, promise.cause().getClass());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }
}
