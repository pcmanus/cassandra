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

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.async.NettyFactory.InboundInitializer;

public class NettyFactoryTest
{
    private static final int receiveBufferSize = 1 << 16;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test(expected = ConfigurationException.class)
    public void createServerChannel_SecondAttemptToBind()
    {
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 9876);
        InboundInitializer inboundInitializer = new InboundInitializer(new AllowAllInternodeAuthenticator(), null);
        NettyFactory.createInboundChannel(addr, inboundInitializer, receiveBufferSize);
        NettyFactory.createInboundChannel(addr, inboundInitializer, receiveBufferSize);
    }

    @Test(expected = ConfigurationException.class)
    public void createServerChannel_UnbindableAddress()
    {
        InetSocketAddress addr = new InetSocketAddress("1.1.1.1", 9876);
        InboundInitializer inboundInitializer = new InboundInitializer(new AllowAllInternodeAuthenticator(), null);
        NettyFactory.createInboundChannel(addr, inboundInitializer, receiveBufferSize);
    }

    @Test
    public void deterineAcceptGroupSize()
    {
        Assert.assertEquals(1, NettyFactory.determineAcceptGroupSize(InternodeEncryption.none));
        Assert.assertEquals(1, NettyFactory.determineAcceptGroupSize(InternodeEncryption.all));
        Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.rack));
        Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.dc));
    }
}
