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
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.locator.Ec2MultiRegionSnitch;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.MessageOut;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size).
 */
public class OutboundMessagingPool
{
    private static final long LARGE_MESSAGE_THRESHOLD = Long.getLong(Config.PROPERTY_PREFIX + "otcp_large_message_threshold", 1024 * 64);

    private final ConnectionMetrics metrics;
    private final BackPressureState backPressureState;

    public OutboundMessagingConnection gossipChannel;
    public OutboundMessagingConnection largeMessageChannel;
    public OutboundMessagingConnection smallMessageChannel;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP addresses
     * which need to be used for communication between EC2 regions.
     */
    private InetSocketAddress preferredRemoteAddr;

    public OutboundMessagingPool(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions, BackPressureState backPressureState)
    {
        preferredRemoteAddr = remoteAddr;
        this.backPressureState = backPressureState;
        metrics = new ConnectionMetrics(localAddr.getAddress(), this);

        smallMessageChannel = new OutboundMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions, true);
        largeMessageChannel = new OutboundMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions, true);

        // don't attempt coalesce the gossip messages, just ship them out asap (let's not anger the FD on any peer node by any artificial delays)
        gossipChannel = new OutboundMessagingConnection(preferredRemoteAddr, localAddr, encryptionOptions, false);
    }

    public BackPressureState getBackPressureState()
    {
        return backPressureState;
    }

    public void sendMessage(MessageOut msg, int id)
    {
        getConnection(msg).enqueue(msg, id);
    }

    private OutboundMessagingConnection getConnection(MessageOut msg)
    {
        if (Stage.GOSSIP == msg.getStage())
            return gossipChannel;

        return msg.payloadSize(smallMessageChannel.getTargetVersion()) > LARGE_MESSAGE_THRESHOLD
             ? largeMessageChannel
             : smallMessageChannel;
    }

    // TODO:JEB add better comment; this is just lifted from OTCP
    /**
     * Reconnect to {@link #preferredRemoteAddr} after the current message backlog is exhausted.
     * Used by classes like {@link Ec2MultiRegionSnitch} to force nodes in the same region to communicate over their private IPs.
     */
    public void reset()
    {
        close(true);
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void reconnectWithNewIp(InetSocketAddress addr)
    {
        preferredRemoteAddr = addr;
        gossipChannel.reconnectWithNewIp(addr);
        largeMessageChannel.reconnectWithNewIp(addr);
        smallMessageChannel.reconnectWithNewIp(addr);
    }

    public void close(boolean softClose)
    {
        gossipChannel.close(softClose);
        largeMessageChannel.close(softClose);
        smallMessageChannel.close(softClose);
    }

    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public long getTimeouts()
    {
        return metrics.timeouts.getCount();
    }

    public InetSocketAddress getPreferredRemoteAddr()
    {
        return preferredRemoteAddr;
    }
}
