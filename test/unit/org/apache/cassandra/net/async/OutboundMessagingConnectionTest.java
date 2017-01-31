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
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType;
import org.apache.cassandra.net.async.OutboundMessagingConnection.State;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult.Result.DISCONNECT;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult.Result.GOOD;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult.Result.NEGOTIATION_FAILURE;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.CREATING_CHANNEL;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.NOT_READY;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.READY;

public class OutboundMessagingConnectionTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9998);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.1", 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final int PENDING_MESSAGES_COUNT = 12;

    private OutboundMessagingConnection omc;
    private EmbeddedChannel channel;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        omc = new OutboundMessagingConnection(ConnectionType.SMALL_MESSAGE, REMOTE_ADDR, LOCAL_ADDR, null, new FakeCoalescingStrategy(false));
        channel = new EmbeddedChannel();
        omc.setChannel(channel);
        omc.setPendingMessages(PENDING_MESSAGES_COUNT);
    }

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void clearBacklog_EmptyBacklog()
    {
        Assert.assertEquals(PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());
        Assert.assertEquals(0, omc.backlogSize());
        omc.writeBacklogToChannel();

        // force a flush to ensure the message's netty callback is invoked
        channel.flush();

        Assert.assertEquals(PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());
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
        Assert.assertEquals(PENDING_MESSAGES_COUNT + 1, omc.getPendingMessages().intValue());
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
    public void handleMessagePromise_FutureNotDone()
    {
        ChannelPromise promise = channel.newPromise();
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void handleMessagePromise_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        omc.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1));

        // message will be retried
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.backlogSize());
        Assert.assertEquals(PENDING_MESSAGES_COUNT + 1, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_ExpiredException_RetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new ExpiredException());
        omc.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1));
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.backlogSize());
        Assert.assertEquals(PENDING_MESSAGES_COUNT + 1, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_ExpiredException_DoNotRetryMsg()
    {
        omc.setState(State.READY);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new ExpiredException());
        omc.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1).retry(System.nanoTime()));
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void handleMessagePromise_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());
    }

    @Test
    public void handleMessagePromise_IOException_ChannelClosed_DoNotRetryMsg()
    {
        omc.setState(State.NOT_READY);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        channel.close();
        omc.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true).retry(System.nanoTime()));

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
        Assert.assertEquals(count + PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());

        omc.close(false);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(State.CLOSED, omc.getState());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(PENDING_MESSAGES_COUNT, omc.getPendingMessages().intValue());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        omc.setState(READY);
        OutboundConnector connector = new OutboundConnector(null, LOCAL_ADDR, REMOTE_ADDR);
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
        OutboundConnector connector = new OutboundConnector(null, LOCAL_ADDR, REMOTE_ADDR);
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
        OutboundConnector connector = new OutboundConnector(null, LOCAL_ADDR, REMOTE_ADDR);
        omc.setOutboundConnector(connector);
        omc.connectionTimeout(connector);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, GOOD, new AtomicLong());
        omc.finishHandshake(result);
        Assert.assertEquals(channel, omc.getChannel());
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

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, DISCONNECT, null);
        omc.finishHandshake(result);
        Assert.assertEquals(channel, omc.getChannel());
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

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, NEGOTIATION_FAILURE, null);
        omc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(0, omc.backlogSize());
    }
}
