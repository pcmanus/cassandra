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

import org.junit.Test;

import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.async.NettyFactory.InboundInitializer;

public class NettyFactoryTest
{
    @Test(expected = ConfigurationException.class)
    public void createServerChannel_SecondAttemptToBind()
    {
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 9876);
        InboundInitializer inboundInitializer = new InboundInitializer(new AllowAllInternodeAuthenticator());
        NettyFactory.createInboundChannel(addr, inboundInitializer);
        NettyFactory.createInboundChannel(addr, inboundInitializer);
    }

    @Test(expected = ConfigurationException.class)
    public void createServerChannel_UnbindableAddress()
    {
        InetSocketAddress addr = new InetSocketAddress("1.1.1.1", 9876);
        InboundInitializer inboundInitializer = new InboundInitializer(new AllowAllInternodeAuthenticator());
        NettyFactory.createInboundChannel(addr, inboundInitializer);
    }
}
