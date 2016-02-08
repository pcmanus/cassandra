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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;
import org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.InternodeMessagingConnection.State;

import static org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult.Result.NEGOTIATION_FAILURE;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult.Result.DISCONNECT;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult.Result.GOOD;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.State.CLOSED;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.State.CREATING_CHANNEL;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.State.NOT_READY;
import static org.apache.cassandra.net.async.InternodeMessagingConnection.State.READY;

public class InternodeMessagingConnectionTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9998);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.1", 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private InternodeMessagingConnection imc;
    private CountingHandler handler;
    private EmbeddedChannel channel;

    @Before
    public void setup()
    {
        imc = new InternodeMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, new FakeCoalescingStrategy(true), new TestScheduledExecutorService());
        handler = new CountingHandler();
        channel = new EmbeddedChannel(handler);
        imc.setChannel(channel);
    }

    @Test
    public void writeAndCount()
    {
        Assert.assertEquals(0, imc.getPendingMessages().intValue());
        imc.writeAndCount(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1));
        Assert.assertEquals(1, imc.getPendingMessages().intValue());
        Assert.assertEquals(1, handler.writeCount);
    }

    @Test
    public void clearBacklog_InactiveChannel()
    {
        channel.close();
        Assert.assertFalse(imc.writeBacklogToChannel());
        Assert.assertEquals(0, imc.getPendingMessages().intValue());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void clearBacklog_EmptyBacklog()
    {
        Assert.assertTrue(imc.writeBacklogToChannel());
        Assert.assertEquals(0, imc.getPendingMessages().intValue());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void clearBacklog_OneMessage()
    {
        imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1));
        Assert.assertTrue(imc.writeBacklogToChannel());
        Assert.assertEquals(1, imc.getPendingMessages().intValue());
        Assert.assertEquals(1, handler.writeCount);
    }

    @Test
    public void clearBacklog_HalfExpiredMessages()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
        {
            if (i % 2 == 0)
                imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
            else
                imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i, 0, true));
        }

        Assert.assertTrue(imc.writeBacklogToChannel());
        int half = 32 >> 1;
        Assert.assertEquals(half, imc.getPendingMessages().intValue());
        Assert.assertEquals(half, handler.writeCount);
        Assert.assertEquals(half, imc.getDroppedMessages().intValue());
    }

    @Test
    public void enqueue_CreatingChannel()
    {
        Assert.assertEquals(0, imc.backlogSize());
        imc.setState(CREATING_CHANNEL);
        imc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(1, imc.backlogSize());
    }

    @Test
    public void enqueue_ChannelClosed()
    {
        Assert.assertEquals(0, imc.backlogSize());
        imc.setState(CLOSED);
        imc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void enqueue_HappyPath()
    {
        Assert.assertEquals(0, imc.backlogSize());
        imc.setState(READY);
        imc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(0, imc.backlogSize());
        Assert.assertEquals(1, imc.getPendingMessages().intValue());
        Assert.assertEquals(1, handler.writeCount);
    }

    @Test
    public void enqueue_ReadyButChannelIsInActive()
    {
        Assert.assertEquals(0, imc.backlogSize());
        imc.setState(READY);
        channel.close();
        channel.pipeline().fireChannelWritabilityChanged();
        imc.enqueue(new MessageOut<>(MessagingService.Verb.ECHO), 1);
        Assert.assertEquals(1, imc.backlogSize());
        Assert.assertEquals(0, imc.getOutboundCount());
        Assert.assertEquals(0, handler.writeCount);
    }

    @Test
    public void handleMessagePromise_FutureNotDone()
    {
        ChannelPromise promise = channel.newPromise();
        imc.handleMessagePromise(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        imc.handleMessagePromise(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        imc.handleMessagePromise(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        imc.handleMessagePromise(promise, null);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_IOException_ShouldCloseChannel_RetryMsg()
    {
        // set this state to not have the test execute the connect() logic
        imc.setState(CLOSED);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        imc.handleMessagePromise(promise, new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1));
        Assert.assertEquals(1, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_IOException_ShouldCloseChannel_DoNotRetryMsg()
    {
        // set this state to not have the test execute the connect() logic
        imc.setState(CLOSED);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        imc.handleMessagePromise(promise, new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1, 0, true));
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void handleMessagePromise_IOException_ChannelClosed_DoNotRetryMsg()
    {
        State state = imc.getState();
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        channel.close();
        imc.handleMessagePromise(promise, new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), 1, 0, true));

        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(0, imc.backlogSize());
        Assert.assertEquals(state, imc.getState());
    }

    @Test
    public void close()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        imc.close();
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(State.CLOSED, imc.getState());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        imc.setState(READY);
        ClientConnector connector = new ClientConnector(null, LOCAL_ADDR, REMOTE_ADDR);
        imc.connectionTimeout(connector);
        Assert.assertEquals(READY, imc.getState());
        Assert.assertFalse(connector.isCancelled());
        Assert.assertEquals(count, imc.backlogSize());
    }

    @Test
    public void connectionTimeout_StateIsNotReady_DiffConnector()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        imc.setState(CREATING_CHANNEL);
        ClientConnector connector = new ClientConnector(null, LOCAL_ADDR, REMOTE_ADDR);
        imc.connectionTimeout(connector);
        Assert.assertEquals(CREATING_CHANNEL, imc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(count, imc.backlogSize());
    }

    @Test
    public void connectionTimeout_StateIsClosed()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        imc.setState(CLOSED);
        ClientConnector connector = new ClientConnector(null, LOCAL_ADDR, REMOTE_ADDR);
        imc.connectionTimeout(connector);
        Assert.assertEquals(CLOSED, imc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(count, imc.backlogSize());
    }

    @Test
    public void connectionTimeout_HappyPath()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        imc.setState(CREATING_CHANNEL);
        ClientConnector connector = new ClientConnector(null, LOCAL_ADDR, REMOTE_ADDR);
        imc.setClientConnector(connector);
        imc.connectionTimeout(connector);
        Assert.assertEquals(NOT_READY, imc.getState());
        Assert.assertTrue(connector.isCancelled());
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        channel.pipeline().addLast(new ClientHandshakeHandler(REMOTE_ADDR, MESSAGING_VERSION, true, null, NettyFactory.Mode.MESSAGING));
        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, GOOD);
        imc.finishHandshake(result);
        Assert.assertEquals(channel, imc.getChannel());
        Assert.assertEquals(READY, imc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, DISCONNECT);
        imc.finishHandshake(result);
        Assert.assertEquals(channel, imc.getChannel());
        Assert.assertEquals(NOT_READY, imc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(count, imc.backlogSize());
    }

    @Test
    public void finishHandshake_CONNECT_FAILURE()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            imc.addToBacklog(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
        Assert.assertEquals(count, imc.backlogSize());

        ConnectionHandshakeResult result = new ConnectionHandshakeResult(channel, MESSAGING_VERSION, NEGOTIATION_FAILURE);
        imc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, imc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(0, imc.backlogSize());
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        ChannelPipeline pipeline = new EmbeddedChannel(new ChannelOutboundHandlerAdapter()).pipeline();
        imc.setupPipeline(pipeline, MESSAGING_VERSION, true);
        Assert.assertNotNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
        Assert.assertNotNull(pipeline.get(CoalescingMessageOutHandler.class));
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        ChannelPipeline pipeline = new EmbeddedChannel(new ChannelOutboundHandlerAdapter()).pipeline();
        imc.setupPipeline(pipeline, MESSAGING_VERSION, false);
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
        Assert.assertNotNull(pipeline.get(CoalescingMessageOutHandler.class));
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
