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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType;

/**
 * A collection of data points to be passed around for outbound connections.
 */
class OutboundConnectionParams
{
    public static final int DEFAULT_SEND_BUFFER_SIZE = 1 << 16;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 1 << 15;

    final InetSocketAddress localAddr;
    final InetSocketAddress remoteAddr;
    final int protocolVersion;
    final Consumer<ConnectionHandshakeResult> callback;
    final ServerEncryptionOptions encryptionOptions;
    final NettyFactory.Mode mode;
    final boolean compress;
    final boolean coalesce;
    final AtomicLong droppedMessageCount;
    final AtomicLong completedMessageCount;
    final AtomicLong pendingMessageCount;
    final ConnectionType connectionType;
    final int sendBufferSize;
    final int receiveBufferSize;
    final boolean tcpNoDelay;

    private OutboundConnectionParams(InetSocketAddress localAddr, InetSocketAddress remoteAddr, int protocolVersion,
                             Consumer<ConnectionHandshakeResult> callback, ServerEncryptionOptions encryptionOptions, NettyFactory.Mode mode,
                             boolean compress, boolean coalesce, AtomicLong droppedMessageCount, AtomicLong completedMessageCount,
                             AtomicLong pendingMessageCount, ConnectionType connectionType, int sendBufferSize, int receiveBufferSize, boolean tcpNoDelay)
    {
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        this.protocolVersion = protocolVersion;
        this.callback = callback;
        this.encryptionOptions = encryptionOptions;
        this.mode = mode;
        this.compress = compress;
        this.coalesce = coalesce;
        this.droppedMessageCount = droppedMessageCount;
        this.completedMessageCount = completedMessageCount;
        this.pendingMessageCount = pendingMessageCount;
        this.connectionType = connectionType;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.tcpNoDelay = tcpNoDelay;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(OutboundConnectionParams params)
    {
        return new Builder(params);
    }
    
    public static class Builder
    {
        private InetSocketAddress localAddr;
        private InetSocketAddress remoteAddr;
        private int protocolVersion;
        private Consumer<ConnectionHandshakeResult> callback;
        private ServerEncryptionOptions encryptionOptions;
        private NettyFactory.Mode mode;
        private boolean compress;
        private boolean coalesce;
        private AtomicLong droppedMessageCount;
        private AtomicLong completedMessageCount;
        private AtomicLong pendingMessageCount;
        private ConnectionType connectionType;
        private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
        private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
        private boolean tcpNoDelay;

        private Builder()
        {   }

        private Builder(OutboundConnectionParams params)
        {
            this.localAddr = params.localAddr;
            this.remoteAddr = params.remoteAddr;
            this.protocolVersion = params.protocolVersion;
            this.callback = params.callback;
            this.encryptionOptions = params.encryptionOptions;
            this.mode = params.mode;
            this.compress = params.compress;
            this.coalesce = params.coalesce;
            this.droppedMessageCount = params.droppedMessageCount;
            this.completedMessageCount = params.completedMessageCount;
            this.pendingMessageCount = params.pendingMessageCount;
            this.connectionType = params.connectionType;
            this.sendBufferSize = params.sendBufferSize;
            this.receiveBufferSize = params.receiveBufferSize;
            this.tcpNoDelay = params.tcpNoDelay;
        }

        public Builder localAddr(InetSocketAddress localAddr)
        {
            this.localAddr = localAddr;
            return this;
        }

        public Builder remoteAddr(InetSocketAddress remoteAddr)
        {
            this.remoteAddr = remoteAddr;
            return this;
        }

        public Builder protocolVersion(int protocolVersion)
        {
            this.protocolVersion = protocolVersion;
            return this;
        }

        public Builder callback(Consumer<ConnectionHandshakeResult> callback)
        {
            this.callback = callback;
            return this;
        }

        public Builder encryptionOptions(ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
            return this;
        }

        public Builder mode(NettyFactory.Mode mode)
        {
            this.mode = mode;
            return this;
        }

        public Builder compress(boolean compress)
        {
            this.compress = compress;
            return this;
        }

        public Builder coalesce(boolean coalesce)
        {
            this.coalesce = coalesce;
            return this;
        }

        public Builder droppedMessageCount(AtomicLong droppedMessageCount)
        {
            this.droppedMessageCount = droppedMessageCount;
            return this;
        }

        public Builder completedMessageCount(AtomicLong completedMessageCount)
        {
            this.completedMessageCount = completedMessageCount;
            return this;
        }

        public Builder pendingMessageCount(AtomicLong pendingMessageCount)
        {
            this.pendingMessageCount = pendingMessageCount;
            return this;
        }

        public Builder connectionType(ConnectionType connectionType)
        {
            this.connectionType = connectionType;
            return this;
        }

        public Builder sendBufferSize(int sendBufferSize)
        {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public Builder receiveBufferSize(int receiveBufferSize)
        {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public Builder tcpNoDelay(boolean tcpNoDelay)
        {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public OutboundConnectionParams build()
        {
            Preconditions.checkArgument(sendBufferSize > 0, "illegal send buffer size: " + sendBufferSize);

            return new OutboundConnectionParams(localAddr, remoteAddr, protocolVersion, callback, encryptionOptions,
                                                mode, compress, coalesce, droppedMessageCount, completedMessageCount,
                                                pendingMessageCount, connectionType, sendBufferSize, receiveBufferSize, tcpNoDelay);
        }
    }
}
