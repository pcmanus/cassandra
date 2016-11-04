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

import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class FlushHandlerTest
{
    private FakeCoalescingStrategy coalescingStrategy;
    private FlushHandler handler;
    private EmbeddedChannel channel;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        coalescingStrategy = new FakeCoalescingStrategy(true);
        handler = new FlushHandler(coalescingStrategy);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void tearDown()
    {
        Assert.assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void write_Closed() throws Exception
    {
        channel.close();

        MessageOut messageOut = new MessageOut(MessagingService.Verb.ECHO);
        ChannelPromise promise = channel.newPromise();
        // directly write to the handler
        channel.writeAndFlush(messageOut, promise);
        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertFalse(coalescingStrategy.coalesceCallbackInvoked);
    }

    @Test
    public void write_NonCoalescingWrite() throws Exception
    {
        coalescingStrategy = new FakeCoalescingStrategy(false);
        handler = new CoalescingMessageOutHandler(coalescingStrategy, droppedMessageCount);
        channel = new EmbeddedChannel(handler);

        QueuedMessage queuedMessage = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = channel.newPromise();
        handler.write(channel.pipeline().firstContext(), queuedMessage, promise);
        Assert.assertTrue(promise.isSuccess());
        Assert.assertTrue(coalescingStrategy.coalesceCallbackInvoked);
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        channel.releaseOutbound(); // throw away any outbound messages
    }

    @Test
    public void write_CoalescingWrite() throws Exception
    {
        QueuedMessage queuedMessage = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = (ChannelPromise)channel.write(queuedMessage);
        Assert.assertTrue(promise.isSuccess());
        Assert.assertTrue(coalescingStrategy.coalesceCallbackInvoked);
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        channel.releaseOutbound(); // throw away any outbound messages
    }

    @Test
    public void doCoalesce_EmptyQueue()
    {
        handler.doCoalesce(channel.pipeline().firstContext());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }

    @Test
    public void doCoalesce_PopulatedQueue()
    {
        int backLogSize = 16;

        for (int i = 0; i < backLogSize; i++)
            handler.addToQueue(new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), i), channel.newPromise());

        int processed = handler.doCoalesce(channel.pipeline().firstContext());
        Assert.assertEquals(backLogSize, processed);
        Assert.assertEquals(backLogSize, channel.outboundMessages().size());
        Assert.assertEquals(0, droppedMessageCount.intValue());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        channel.releaseOutbound(); // throw away any outbound messages
    }

    @Test
    public void doCoalesce_PopulatedQueueWithSomeExpired()
    {
        int backLogSize = 16;

        for (int i = 0; i < backLogSize; i++)
        {
            if (i % 2 == 0)
                handler.addToQueue(new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), i), channel.newPromise());
            else
                handler.addToQueue(new QueuedMessage(new MessageOut(MessagingService.Verb.REQUEST_RESPONSE), i, 0, true), channel.newPromise());
        }

        int processed = handler.doCoalesce(channel.pipeline().firstContext());
        Assert.assertEquals(backLogSize, processed);
        Assert.assertEquals(backLogSize / 2, channel.outboundMessages().size());
        Assert.assertEquals(backLogSize / 2, droppedMessageCount.intValue());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        channel.releaseOutbound(); // throw away any outbound messages
    }
}
