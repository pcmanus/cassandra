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

import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;

public class OutboundConnectorTest
{
    private final static InetSocketAddress local = InetSocketAddress.createUnresolved("127.0.0.1", 9876);
    private final static InetSocketAddress remote = InetSocketAddress.createUnresolved("127.0.0.2", 9876);

    EmbeddedChannel channel;
    OutboundConnector connector;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        connector = new OutboundConnector(null, local, remote);
    }

    @After
    public void tearDown()
    {
        Assert.assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void connectComplete_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        Assert.assertTrue(connector.connectCallback(promise));
    }

    @Test
    public void connectComplete_FutureIsCancelled()
    {
        ChannelFuture future = channel.newPromise();
        future.cancel(false);
        Assert.assertFalse(connector.connectCallback(future));
    }

    @Test
    public void connectComplete_ConnectorIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        connector.cancel();
        Assert.assertFalse(connector.connectCallback(promise));
    }

    @Test
    public void connectComplete_FailCauseIsSslHandshake()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new SSLHandshakeException("test is only a test"));
        Assert.assertFalse(connector.connectCallback(promise));
    }

    @Test
    public void connectComplete_FailCauseIsNPE()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("test is only a test"));
        Assert.assertFalse(connector.connectCallback(promise));
    }

    @Test
    public void connectComplete_FailCauseIsIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        Assert.assertFalse(connector.connectCallback(promise));
    }
}
