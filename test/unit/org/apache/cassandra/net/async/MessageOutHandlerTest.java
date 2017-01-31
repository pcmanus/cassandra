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
import java.util.concurrent.atomic.AtomicLong;

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

import static org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType.SMALL_MESSAGE;

public class MessageOutHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private EmbeddedChannel channel;
    private MessageOutHandler handler;
    private AtomicLong completedMessages = new AtomicLong(0);
    private AtomicLong pendingMessages = new AtomicLong(0);
    private AtomicLong droppedMessages = new AtomicLong(0);

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        completedMessages.set(0);
        droppedMessages.set(0);
        pendingMessages.set(0);
        droppedMessages = new AtomicLong(0);
        handler = new MessageOutHandler(new InetSocketAddress("127.0.0.1", 0), MESSAGING_VERSION, completedMessages,
                                        pendingMessages, false, SMALL_MESSAGE, droppedMessages);
        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void serializeMessage() throws IOException
    {
        pendingMessages.incrementAndGet();
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertEquals(1, completedMessages.get());
        Assert.assertEquals(0, pendingMessages.get());
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
        Assert.assertEquals(1, droppedMessages.get());
    }

    @Test
    public void unexpiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertTrue(handler.isMessageValid(msg, promise));

        // we won't know if it was successful yet, but we'll know if it's a failure because cause will be set
        Assert.assertNull(promise.cause());
        Assert.assertEquals(0, droppedMessages.get());
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
        Assert.assertEquals(1, droppedMessages.get());
    }
}
