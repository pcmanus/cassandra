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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class MessageOutTimeoutHandlerTest
{
    private AtomicLong droppedMessages;
    private EmbeddedChannel channel;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        droppedMessages = new AtomicLong(0);
        channel = new EmbeddedChannel(new MessageOutTimeoutHandler(droppedMessages));
    }

    @Test
    public void wrongMessageType()
    {
        ChannelFuture future = channel.writeAndFlush("this is the wrong message type");

        Assert.assertFalse(future.isSuccess());
        Assert.assertNotNull(future.cause());
        Assert.assertSame(UnsupportedMessageTypeException.class, future.cause().getClass());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
        Assert.assertEquals(0, droppedMessages.get());
    }

    @Test
    public void unexpiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertTrue(future.isSuccess());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        Assert.assertEquals(0, droppedMessages.get());
    }

    @Test
    public void expiredMessage()
    {
        AtomicLong droppedMessages = new AtomicLong(0);
        MessageOutTimeoutHandler handler = new MessageOutTimeoutHandler(droppedMessages);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1, 0, true, true);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertFalse(future.isSuccess());
        Assert.assertNotNull(future.cause());
        Assert.assertSame(ExpiredException.class, future.cause().getClass());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
        Assert.assertEquals(1, droppedMessages.get());
    }
}
