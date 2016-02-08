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

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.utils.CoalescingStrategies;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size).
 */
public class InternodeMessagingPool
{
    private final ConnectionMetrics metrics;

    public InternodeMessagingConnection gossipChannel;
    public InternodeMessagingConnection largeMessageChannel;
    public InternodeMessagingConnection smallMessageChannel;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP addresses
     * which need to be used for communication between EC2 regions.
     */
    private InetSocketAddress preferredRemoteAddr;

    public InternodeMessagingPool(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions)
    {
        preferredRemoteAddr = remoteAddr;
        metrics = new ConnectionMetrics(localAddr.getAddress(), this);

        String displayName = preferredRemoteAddr.getAddress().getHostAddress();
        smallMessageChannel = new InternodeMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions,
                                                               newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(), displayName));
        largeMessageChannel = new InternodeMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions,
                                                               newCoalescingStrategy(DatabaseDescriptor.getOtcCoalescingStrategy(), displayName));

        // don't coalesce the gossip messages, just ship them out asap (let's not piss off the FD on any peer node by any artificial delays)
        gossipChannel = new InternodeMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions,
                                                         newCoalescingStrategy("DISABLED", displayName));

    }

    private static CoalescingStrategies.CoalescingStrategy newCoalescingStrategy(String strategyName, String displayName)
    {
        return CoalescingStrategies.newCoalescingStrategy(strategyName,
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          InternodeMessagingConnection.logger,
                                                          displayName);
    }

    public void sendMessage(MessageOut msg, int id)
    {
        getConnection(msg).enqueue(msg, id);
    }

    private InternodeMessagingConnection getConnection(MessageOut msg)
    {
        if (Stage.GOSSIP == msg.getStage())
            return gossipChannel;

        return msg.payloadSize(smallMessageChannel.getTargetVersion()) > OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD
             ? largeMessageChannel
             : smallMessageChannel;
    }

    /**
     * functionally equivalent to {@link OutboundTcpConnectionPool#reset()}
     */
    public void reset()
    {
        close();
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void switchIpAddress(InetSocketAddress addr)
    {
        preferredRemoteAddr = addr;
        gossipChannel.switchIpAddress(addr);
        largeMessageChannel.switchIpAddress(addr);
        smallMessageChannel.switchIpAddress(addr);
    }

    public void close()
    {
        gossipChannel.close();
        largeMessageChannel.close();
        smallMessageChannel.close();
    }

    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public InetSocketAddress getPreferredRemoteAddr()
    {
        return preferredRemoteAddr;
    }
}
