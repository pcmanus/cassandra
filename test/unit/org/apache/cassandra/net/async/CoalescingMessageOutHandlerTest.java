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

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;

public class CoalescingMessageOutHandlerTest
{
    private final AtomicLong droppedMessageCount = new AtomicLong(0);
    private FakeCoalescingStrategy coalescingStrategy;

    @Before
    public void setup()
    {
        coalescingStrategy = new FakeCoalescingStrategy(true);
        droppedMessageCount.set(0);
    }

    @Test
    public void write_Closed() throws Exception
    {
        AtomicLong queueSize = new AtomicLong(1);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.close();

        MessageOut messageOut = new MessageOut(MessagingService.Verb.ECHO);
        ChannelPromise promise = channel.newPromise();
        // directly write to the handler
        channel.writeAndFlush(messageOut, promise);
        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertFalse(coalescingStrategy.coalesceCallbackInvoked);
        Assert.assertEquals(1, queueSize.intValue());
        Assert.assertEquals(0, droppedMessageCount.intValue());
    }

    @Test
    public void write_NonCoalescingWrite() throws Exception
    {
        AtomicLong queueSize = new AtomicLong(1);
        FakeCoalescingStrategy coalescingStrategy = new FakeCoalescingStrategy(false);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        QueuedMessage queuedMessage = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = channel.newPromise();
        handler.write(channel.pipeline().firstContext(), queuedMessage, promise);
        Assert.assertTrue(promise.isSuccess());
        Assert.assertTrue(coalescingStrategy.coalesceCallbackInvoked);
        Assert.assertEquals(0, droppedMessageCount.intValue());
        Assert.assertEquals(0, queueSize.intValue());
    }

    @Test
    public void write_CoalescingWrite() throws Exception
    {
        AtomicLong queueSize = new AtomicLong(1);
        FakeCoalescingStrategy coalescingStrategy = new FakeCoalescingStrategy(true);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        QueuedMessage queuedMessage = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = (ChannelPromise)channel.write(queuedMessage);
        Assert.assertTrue(promise.isSuccess());
        Assert.assertTrue(coalescingStrategy.coalesceCallbackInvoked);
        Assert.assertEquals(0, queueSize.intValue());
        Assert.assertEquals(0, droppedMessageCount.intValue());
    }

    @Test
    public void doCoalesce_EmptyQueue()
    {
        AtomicLong queueSize = new AtomicLong(0);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        handler.doCoalesce(channel.pipeline().firstContext());
        Assert.assertEquals(0, queueSize.intValue());
        Assert.assertEquals(0, droppedMessageCount.intValue());
    }

    @Test
    public void doCoalesce_PopulatedQueue()
    {
        int backLogSize = 16;
        AtomicLong queueSize = new AtomicLong(backLogSize);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        for (int i = 0; i < backLogSize; i++)
            handler.addToQueue(new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), i), channel.newPromise());

        int processed = handler.doCoalesce(channel.pipeline().firstContext());
        Assert.assertEquals(backLogSize, processed);
        Assert.assertEquals(backLogSize, channel.outboundMessages().size());
        Assert.assertEquals(0, droppedMessageCount.intValue());
    }

    @Test
    public void doCoalesce_PopulatedQueueWithSomeExpired()
    {
        int backLogSize = 16;
        AtomicLong queueSize = new AtomicLong(backLogSize);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

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
    }

    @Test
    public void iterator()
    {
        int backLogSize = 16;
        AtomicLong queueSize = new AtomicLong(backLogSize);
        CoalescingMessageOutHandler handler = new CoalescingMessageOutHandler(coalescingStrategy, queueSize, droppedMessageCount);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        for (int i = 0; i < backLogSize; i++)
            handler.addToQueue(new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), i), channel.newPromise());

        Assert.assertEquals(backLogSize, Iterators.size(handler.iterator()));
    }
}
