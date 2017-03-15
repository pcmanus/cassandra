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
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.State;
import org.apache.cassandra.utils.CoalescingStrategies;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.CREATING_CHANNEL;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.NOT_READY;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.READY;

public class OutboundMessagingConnectionTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9998);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.1", 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private OutboundConnectionIdentifier connectionId;
    private OutboundMessagingConnection omc;
    private EmbeddedChannel channel;
    private OutChannel outChannel;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        connectionId = OutboundConnectionIdentifier.small(LOCAL_ADDR, REMOTE_ADDR);
        CoalescingStrategies.CoalescingStrategy strategy = new FakeCoalescingStrategy(false);
        omc = new OutboundMessagingConnection(connectionId, null, strategy);
        channel = new EmbeddedChannel();
        outChannel = OutChannel.create(channel, strategy);
        omc.setOutChannel(outChannel);
    }

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void clearBacklog_EmptyBacklog()
    {
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(0, omc.backlogSize());
        omc.writeBacklogToChannel();

        // force a flush to ensure the message's netty callback is invoked
        channel.flush();

        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void clearBacklog_OneMessage()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), 1));
        Assert.assertEquals(1, omc.backlogSize());

        omc.writeBacklogToChannel();
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void enqueue_CreatingChannel()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(CREATING_CHANNEL);
        omc.sendMessage(new MessageOut<>(ECHO), 1);
        Assert.assertEquals(1, omc.backlogSize());
        Assert.assertEquals(1, omc.getPendingMessages().intValue());
    }

    @Test
    public void enqueue_HappyPath()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(READY);
        omc.sendMessage(new MessageOut<>(ECHO), 1);

        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void handleMessagePromise_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        outChannel.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1), omc);

        // message will be retried
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_ExpiredException_DoNotRetryMsg()
    {
        omc.setState(State.READY);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new ExpiredException());
        outChannel.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1), omc);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void handleMessagePromise_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        outChannel.handleMessageFuture(promise, null, omc);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_IOException_ChannelClosed_DoNotRetryMsg()
    {
        omc.setState(State.NOT_READY);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        channel.close();
        outChannel.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), omc);

        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(1, omc.backlogSize());
    }

    @Test
    public void close()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());
        Assert.assertEquals(count, omc.getPendingMessages().intValue());

        omc.close(false);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(State.CLOSED, omc.getState());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        omc.setState(READY);
        OutboundConnector connector = new OutboundConnector(null, connectionId);
        omc.connectionTimeout(connector);
        Assert.assertEquals(READY, omc.getState());
        Assert.assertFalse(connector.isCancelled());
        Assert.assertEquals(count, omc.backlogSize());
    }

    @Test
    public void connectionTimeout_StateIsNotReady_DiffConnector()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        omc.setState(CREATING_CHANNEL);
        OutboundConnector connector = new OutboundConnector(null, connectionId);
        omc.connectionTimeout(connector);
        Assert.assertEquals(CREATING_CHANNEL, omc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(count, omc.backlogSize());
    }

    @Test
    public void connectionTimeout_HappyPath()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        omc.setState(CREATING_CHANNEL);
        OutboundConnector connector = new OutboundConnector(null, connectionId);
        omc.setOutboundConnector(connector);
        omc.connectionTimeout(connector);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        HandshakeResult result = HandshakeResult.success(outChannel, MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertEquals(outChannel, omc.getOutChannel());
        Assert.assertEquals(READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        HandshakeResult result = HandshakeResult.disconnect(MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(count, omc.backlogSize());
    }

    @Test
    public void finishHandshake_CONNECT_FAILURE()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        HandshakeResult result = HandshakeResult.failed();
        omc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(0, omc.backlogSize());
    }
}
