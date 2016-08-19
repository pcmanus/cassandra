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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.NettyFactory.ClientChannelInitializer;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.net.OutboundTcpConnection.isLocalDC;

/**
 * Asynchronously (via netty) connects to a remote peer. On connection failures, will attempt to reconnect
 * unless cancelled by an external caller.
 */
class ClientConnector
{
    private static final Logger logger = LoggerFactory.getLogger(ClientConnector.class);

    /**
     * We can keep the {@link Bootstrap} around in case we need to reconnect to the peer,
     * assuming, of course, we aren't switching IP addresses, and so on...
     */
    private final Bootstrap bootstrap;
    private final InetSocketAddress localAddr;
    private final InetSocketAddress remoteAddr;

    private volatile boolean isCancelled;
    private int connectAttemptCount;

    /**
     * A reference to the {@link Future} that is returned from the {@link Bootstrap#connect()} attempt.
     */
    private volatile ChannelFuture connectFuture;

    @VisibleForTesting
    ClientConnector(Bootstrap bootstrap, @Nullable InetSocketAddress localAddr, InetSocketAddress remoteAddr)
    {
        this.bootstrap = bootstrap;
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
    }

    public void connect()
    {
        if (isCancelled)
            return;
        connectAttemptCount++;
        logger.debug("attempting to connect to {}", remoteAddr);
        connectFuture = localAddr == null ? bootstrap.connect(remoteAddr) : bootstrap.connect(remoteAddr, localAddr);
        connectFuture.addListener(this::connectComplete);
    }

    /**
     * Handles the callback of the connection attempt. If the future is cancelled (or {@link #isCancelled} is true), close the channel
     * (which should disconnect the socket, if connected). If there was an {@link IOException} while trying to connect,
     * the connection will be retried after a short delay.
     */
    boolean connectComplete(Future<? super Void> future)
    {
        if (!future.isDone())
            return false;

        ChannelFuture channelFuture = (ChannelFuture)future;
        if (channelFuture.isCancelled() || isCancelled)
        {
            channelFuture.channel().close();
            return false;
        }

        if (channelFuture.isSuccess())
            return true;

        Throwable cause = future.cause();
        if (cause instanceof IOException)
        {
            logger.trace("unable to connect to {}", remoteAddr, cause);

            // it's safe to get a reference to the channel from the Future instance, thus we can get the executor
            channelFuture.channel().eventLoop().schedule(this::connect, OutboundTcpConnection.OPEN_RETRY_DELAY * connectAttemptCount, TimeUnit.MILLISECONDS);
        }
        else
        {
            JVMStabilityInspector.inspectThrowable(cause);
            logger.error("non-IO error attempting to connect to {}", remoteAddr, cause);
        }
        return false;
    }

    /**
     * Attempt to cancel the connection attempt.
     *
     * Note: this will execute on a different thread than the netty IO thread.
     */
    public void cancel()
    {
        isCancelled = true;
        ChannelFuture future = connectFuture;
        if (future != null)
            future.cancel(false);
    }

    public static class Builder
    {
        private InetSocketAddress localAddr;
        private InetSocketAddress remoteAddr;
        private int channelBufferSize;

        // even though this is the "client" end of this interaction, we're dealing with the server-to-server internode
        // communications here, so thus we need the "server" EncryptionOptions.
        private ServerEncryptionOptions encryptionOptions;
        private int protocolVersion;
        private boolean compress;
        private Consumer<ConnectionHandshakeResult> callback;
        private NettyFactory.Mode mode = NettyFactory.Mode.MESSAGING;

        public Builder addLocalAddr(InetSocketAddress localAddr)
        {
            this.localAddr = localAddr;
            return this;
        }

        public Builder addRemoteAddr(InetSocketAddress remoteAddr)
        {
            this.remoteAddr = remoteAddr;
            return this;
        }

        public Builder channelBufferSize(int channelBufferSize)
        {
            this.channelBufferSize = channelBufferSize;
            return this;
        }

        public Builder compress(boolean compress)
        {
            this.compress = compress;
            return this;
        }

        public Builder protocolVersion(int protocolVersion)
        {
            this.protocolVersion = protocolVersion;
            return this;
        }

        public Builder encryptionOptions(ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
            return this;
        }

        public Builder streaming()
        {
            mode = NettyFactory.Mode.STREAMING;
            return this;
        }

        public Builder callback(Consumer<ConnectionHandshakeResult> callback)
        {
            this.callback = callback;
            return this;
        }

        ClientConnector build()
        {
            ClientChannelInitializer initializer = new ClientChannelInitializer(remoteAddr, protocolVersion, compress, callback, encryptionOptions, mode);
            return new ClientConnector(buildBootstrap(remoteAddr, initializer, channelBufferSize), localAddr, remoteAddr);
        }
    }

    private static Bootstrap buildBootstrap(InetSocketAddress remoteAddr, ClientChannelInitializer initializer, int channelBufferSize)
    {
        boolean tcpNoDelay;
        if (isLocalDC(remoteAddr.getAddress()))
            tcpNoDelay = OutboundTcpConnection.INTRADC_TCP_NODELAY;
        else
            tcpNoDelay = DatabaseDescriptor.getInterDCTcpNoDelay();

        int sendBufferSize = 1 << 16;
        if (DatabaseDescriptor.getInternodeSendBufferSize() != null)
            sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize();

        return NettyFactory.createClientChannel(initializer, sendBufferSize, tcpNoDelay, channelBufferSize);
    }

    @VisibleForTesting
    boolean isCancelled()
    {
        return isCancelled;
    }
}
