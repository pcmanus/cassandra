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

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.SecondHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.ThirdHandshakeMessage;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;

/**
 * A {@link ChannelHandler} to execute the send-side of the internode communication handshake protocol.
 * As soon as the handler is added to the channel via {@link #channelActive(ChannelHandlerContext)}
 * (which is only invoked if the underlying connection was properly established), the first message of
 * the internode messaging protocol is automatically sent out.
 *
 * Upon completion of the handshake (on success, fail or timeout), the {@link #callback} is invoked to let the listener
 * know the result of the connect/handshake.
 *
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because after the
 * first message is sent on becoming active in the channel, it waits for the peer's response (the second message
 * of the internode messaging handshake protocol).
 */
class OutboundHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundHandshakeHandler.class);

    private static final int LZ4_HASH_SEED = 0x9747b28c;

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The expected messaging service version to use.
     */
    private final int messagingVersion;

    /**
     * A function to invoke upon completion of the attempt, success or failure, to connect to the peer.
     */
    private final Consumer<ConnectionHandshakeResult> callback;
    private final NettyFactory.Mode mode;
    private final OutboundConnectionParams params;

    /**
     * A future that places a timeout on how long we'll wait for the peer to respond
     * so we can move on to the next step of the handshake or just fail.
     */
    private Future<?> timeoutFuture;

    private boolean isCancelled;

    OutboundHandshakeHandler(OutboundConnectionParams params)
    {
        this(params, MessagingService.instance().getVersion(params.connectionId.remote()));
    }

    @VisibleForTesting
    OutboundHandshakeHandler(OutboundConnectionParams params, int messagingVersion)
    {
        this.params = params;
        this.connectionId = params.connectionId;
        this.messagingVersion = messagingVersion;
        this.callback = params.callback;
        this.mode = params.mode;
    }

    // invoked when the channel is active, and sends out the first handshake message
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("starting handshake with {}", connectionId.connectionAddress());
        ctx.writeAndFlush(new FirstHandshakeMessage(messagingVersion, mode, params.compress).encode(ctx.alloc()));

        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        timeoutFuture = ctx.executor().schedule(() -> abortHandshake(ctx), timeout, TimeUnit.MILLISECONDS);
        ctx.fireChannelActive();
    }

    /**
     * {@inheritDoc}
     *
     * Invoked when we get the response back from the peer, which should contain the second message of the internode messaging handshake.
     * <p>
     * If the peer's protocol version is less than what we were expecting, immediately close the channel (and socket); do *not* send out
     * the third message of the internode messaging handshake (the higher protocol may have changed!).
     * <p>
     * If the peer's protocol version is greater than what we were expecting, send out the third message of the internode messaging handshake,
     * but then close the connection. We will reconnect on the appropriate protocol version.
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        if (isCancelled)
            return;

        SecondHandshakeMessage msg = SecondHandshakeMessage.maybeDecode(in);
        if (msg == null)
            return;

        if (timeoutFuture != null)
        {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }

        final int peerMessagingVersion = msg.messagingVersion;

        // we expected a higher protocol version, but it was actually lower
        if (messagingVersion > peerMessagingVersion)
        {
            logger.trace("peer's max version is {}; will reconnect with that version", peerMessagingVersion);
            ctx.close();
            callback.accept(ConnectionHandshakeResult.disconnect(peerMessagingVersion));
            return;
        }
        // we anticipate a version that is lower than what peer is actually running
        else if (messagingVersion < peerMessagingVersion && messagingVersion < MessagingService.current_version)
        {
            logger.trace("peer has a higher max version than expected {} (previous value {})", peerMessagingVersion, messagingVersion);
            ctx.close();
            callback.accept(ConnectionHandshakeResult.disconnect(peerMessagingVersion));
            return;
        }

        try
        {
            ctx.writeAndFlush(new ThirdHandshakeMessage(MessagingService.current_version, connectionId.local()).encode(ctx.alloc()));
            OutChannel outChannel = setupPipeline(ctx.channel(), peerMessagingVersion);
            callback.accept(ConnectionHandshakeResult.success(outChannel, peerMessagingVersion));
        }
        catch (Exception e)
        {
            logger.info("failed to write last internode messaging handshake message", e);
            ctx.close();
            callback.accept(ConnectionHandshakeResult.failed());
        }
    }

    OutChannel setupPipeline(Channel channel, int messagingVersion)
    {
        ChannelPipeline pipeline = channel.pipeline();
        if (params.compress)
            pipeline.addLast(NettyFactory.OUTBOUND_COMPRESSOR_HANDLER_NAME, new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, 1 << 14,
                                                                                                XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum()));

        OutChannel outChannel = OutChannel.create(channel, params.coalescingStrategy);
        pipeline.addLast("messageOutHandler", new MessageOutHandler(connectionId, messagingVersion, outChannel));
        pipeline.remove(this);
        return outChannel;
    }

    /**
     * Handles the timeout when we do not receive a response to the handshake request.
     *
     * Note: This will happen on the netty IO thread, so there no races with {@link #decode(ChannelHandlerContext, ByteBuf, List)}.
     */
    private void abortHandshake(ChannelHandlerContext ctx)
    {
        if (isCancelled)
            return;

        isCancelled = true;
        ctx.close();
        if (callback != null)
            callback.accept(ConnectionHandshakeResult.failed());

        if (timeoutFuture != null)
            timeoutFuture.cancel(false);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        abortHandshake(ctx);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("exception in negotiating internode handshake", cause);
        abortHandshake(ctx);
        ctx.fireExceptionCaught(cause);
    }

    @VisibleForTesting
    boolean isCancelled()
    {
        return isCancelled;
    }
}
