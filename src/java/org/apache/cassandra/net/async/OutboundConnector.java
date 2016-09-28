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
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Asynchronously connects to a remote peer, via netty. The sole responsibility of this class is to
 * establish a TCP socket connection. On connection failures, it will retry the connect unless cancelled by an external caller.
 */
class OutboundConnector
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundConnector.class);

    /**
     * Number of milliseconds between connection retry attempts.
     */
    private static final int OPEN_RETRY_DELAY = 100;

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
    OutboundConnector(Bootstrap bootstrap, @Nullable InetSocketAddress localAddr, InetSocketAddress remoteAddr)
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
        connectFuture.addListener(this::connectCallback);
    }

    /**
     * Handles the callback of the connection attempt. If the future is cancelled (or {@link #isCancelled} is true), close the channel
     * (which should disconnect the socket, if connected). If there was an {@link IOException} while trying to connect,
     * the connection will be retried after a short delay.
     */
    @VisibleForTesting
    boolean connectCallback(Future<? super Void> future)
    {
        if (!future.isDone())
            return false;

        ChannelFuture channelFuture = (ChannelFuture)future;
        if (channelFuture.isSuccess())
            return true;

        if (channelFuture.isCancelled() || isCancelled)
        {
            channelFuture.channel().close();
            return false;
        }

        Throwable cause = future.cause();
        if (cause instanceof IOException)
        {
            logger.trace("unable to connect to {}", remoteAddr, cause);

            // it's safe to get a reference to the channel from the Future instance, thus we can get the executor
            channelFuture.channel().eventLoop().schedule(this::connect, OPEN_RETRY_DELAY * connectAttemptCount, TimeUnit.MILLISECONDS);
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

    @VisibleForTesting
    boolean isCancelled()
    {
        return isCancelled;
    }
}
