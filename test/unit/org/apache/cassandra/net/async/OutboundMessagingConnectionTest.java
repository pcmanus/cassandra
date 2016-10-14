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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.State;

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

    private OutboundMessagingConnection omc ;
    private CountingHandler handler;
    private EmbeddedChannel channel;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        omc = new OutboundMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, false, new TestScheduledExecutorService());
        handler = new CountingHandler();
        channel = new EmbeddedChannel(handler);
        omc.setChannel(channel);
    }

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void clearBacklog_InactiveChannel()
    {
        channel.close();
        Assert.assertFalse(omc.writeBacklogToChannel());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void clearBacklog_EmptyBacklog()
    {
        Assert.assertTrue(omc.writeBacklogToChannel());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void clearBacklog_OneMessage()
    {
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1));
        Assert.assertTrue(omc.writeBacklogToChannel());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(1, handler.writeCount);
    }

    @Test
    public void clearBacklog_HalfExpiredMessages()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
        {
            if (i % 2 == 0)
                omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
            else
                omc.addToBacklog(new RetriedQueuedMessage(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i), Long.MIN_VALUE, true));
        }

        Assert.assertTrue(omc.writeBacklogToChannel());
        int half = 32 >> 1;
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(half, handler.writeCount);
        Assert.assertEquals(half, omc.getDroppedMessages().intValue());
    }

    @Test
    public void enqueue_CreatingChannel()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(CREATING_CHANNEL);
        omc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(1, omc.backlogSize());
    }

    @Test
    public void enqueue_HappyPath()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(READY);
        omc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        Assert.assertEquals(1, handler.writeCount);
    }

    @Test
    public void enqueue_ReadyButChannelIsInActive()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(READY);
        channel.close();
        channel.pipeline().fireChannelWritabilityChanged();
        omc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(1, omc.backlogSize());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void handleMessagePromise_FutureNotDone()
    {
        ChannelPromise promise = channel.newPromise();
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void handleMessagePromise_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void handleMessagePromise_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void handleMessagePromise_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        omc.handleMessageFuture(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void handleMessagePromise_IOException_ChannelClosed_DoNotRetryMsg()
    {
        State state = omc.getState();
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        channel.close();
        omc.handleMessageFuture(promise, new RetriedQueuedMessage(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1, 0, true)));

        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(state, omc.getState());
    }

    @Test
    public void close()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        omc.close(false);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(State.NOT_READY, omc.getState());
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
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
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
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
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
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
        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, GOOD);
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
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, DISCONNECT);
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
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, NEGOTIATION_FAILURE);
        omc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(0, omc.backlogSize());
    }

    static class CountingHandler extends ChannelOutboundHandlerAdapter
    {
        int writeCount;

        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        {
            writeCount++;
            ctx.write(msg, promise);
        }
    }

    // a simple ScheduledExecutorService that throws away everything
    private static class TestScheduledExecutorService implements ScheduledExecutorService
    {
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return null;
        }

        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return null;
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return null;
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return null;
        }

        public void shutdown()
        {

        }

        public List<Runnable> shutdownNow()
        {
            return null;
        }

        public boolean isShutdown()
        {
            return false;
        }

        public boolean isTerminated()
        {
            return false;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return false;
        }

        public <T> Future<T> submit(Callable<T> task)
        {
            return null;
        }

        public <T> Future<T> submit(Runnable task, T result)
        {
            return null;
        }

        public Future<?> submit(Runnable task)
        {
            return null;
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
        {
            return null;
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
        {
            return null;
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
            return null;
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        public void execute(Runnable command)
        {

        }
    }
}
