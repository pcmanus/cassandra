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

import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult.Result;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link ChannelHandler} to execute the send-side of the internode communication handshake protocol.
 * As soon as the handler is added to the channel via {@link #channelActive(ChannelHandlerContext)}
 * (which is only invoked if the underlying connectionwas properly established), the first message of
 * the internode messaging protocol is automatically sent out.
 *
 * Upon completion of the handshake (on success, fail or timeout), the {@link #callback} is invoked to let the listener
 * know the result of th connect/handshake.
 *
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because after the
 * first message is sent on becoming active in the channel, it waits for the peer's response (the second message
 * of the internode messaging handshake protocol).
 */
class OutboundHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundHandshakeHandler.class);

    /**
     * The length of the first message of the internode messaging handshake.
     */
    static final int FIRST_MESSAGE_LENGTH = 8;

    /**
     * The length of the second message of the internode messaging handshake.
     */
    static final int SECOND_MESSAGE_LENGTH = 4;

    /**
     * The length of the third message in the internode message handshake protocol. We need to receive an int (version)
     * and an IP addr. If IPv4, that's 5 more bytes; if IPv6, it's 17 more bytes. Since we can't know apriori if the IP address
     * will be v4 or v6, go with the minimum requires bytes (5), and hope that if the address is v6, we'll have the extra 12 bytes in the packet.
     */
    static final int THIRD_MESSAGE_LENGTH_MIN = 9;

    private static final int LZ4_HASH_SEED = 0x9747b28c;

    /**
     * The IP address to identify this node to the peer. Memoizing this value eliminates a dependency on {@link FBUtilities#getBroadcastAddress()}.
     */
    private final InetSocketAddress localAddr;

    private final InetSocketAddress remoteAddr;

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
        this.params = params;
        this.localAddr = params.localAddr;
        this.remoteAddr = params.remoteAddr;
        this.messagingVersion = params.protocolVersion;
        this.callback = params.callback;
        this.mode = params.mode;
    }

    // invoked when the channel is active, and sends out the first handshake message
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("starting handshake with {}", remoteAddr);
        ctx.writeAndFlush(firstHandshakeMessage(ctx.alloc().buffer(FIRST_MESSAGE_LENGTH), createHeader(messagingVersion, params.compress, mode)));
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        timeoutFuture = ctx.executor().schedule(() -> abortHandshake(ctx), timeout, TimeUnit.MILLISECONDS);
        ctx.fireChannelActive();
    }

    @VisibleForTesting
    static ByteBuf firstHandshakeMessage(ByteBuf handshakeBuf, int header)
    {
        handshakeBuf.writeInt(MessagingService.PROTOCOL_MAGIC);
        handshakeBuf.writeInt(header);
        return handshakeBuf;
    }

    @VisibleForTesting
    static int createHeader(int version, boolean compressionEnabled, NettyFactory.Mode mode)
    {
        int header = 0;
        if (compressionEnabled)
            header |= 1 << 2;
        if (mode == NettyFactory.Mode.STREAMING)
            header |= 8;

        header |= (version << 8);
        return header;
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

        if (in.readableBytes() < SECOND_MESSAGE_LENGTH)
            return;

        if (timeoutFuture != null)
        {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }

        final int peerMessagingVersion = in.readInt();
        // we expected a higher protocol version, but it was actually lower
        if (messagingVersion > peerMessagingVersion)
        {
            logger.trace("peer's max version is {}; will reconnect with that version", peerMessagingVersion);
            ctx.close();
            callback.accept(new ConnectionHandshakeResult(ctx.channel(), peerMessagingVersion, Result.DISCONNECT));
            return;
        }

        ByteBuf buf = ctx.alloc().buffer(Integer.BYTES + CompactEndpointSerializationHelper.serializedSize(localAddr.getAddress()));
        Result result;
        try
        {
            buf.writeInt(MessagingService.current_version);
            @SuppressWarnings("resource")
            DataOutput bbos = new ByteBufOutputStream(buf);
            CompactEndpointSerializationHelper.serialize(localAddr.getAddress(), bbos);
            ctx.writeAndFlush(buf);
            setupPipeline(ctx.channel().pipeline(), peerMessagingVersion);
            ctx.channel().pipeline().remove(this);
            result = Result.GOOD;
        }
        catch (Exception e)
        {
            logger.info("failed to write last internode messaging handshake message", e);
            buf.release();
            ctx.close();
            result = Result.NEGOTIATION_FAILURE;
        }

        // we anticipate a version that is lower than what peer is actually running
        if (messagingVersion < peerMessagingVersion && messagingVersion < MessagingService.current_version)
        {
            logger.trace("peer has a higher max version than expected {} (previous value {})", peerMessagingVersion, messagingVersion);
            result = Result.DISCONNECT;
            ctx.close();
        }

        callback.accept(new ConnectionHandshakeResult(ctx.channel(), peerMessagingVersion, result));
    }

    void setupPipeline(ChannelPipeline pipeline, int messagingVersion)
    {
        if (params.compress)
            pipeline.addLast(NettyFactory.OUTBOUND_COMPRESSOR_HANDLER_NAME, new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, 1 << 14,
                                                                                                XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum()));

        // only create an updated params instance if the messaging versions are different
        OutboundConnectionParams updatedParams = messagingVersion == params.protocolVersion ? params : params.updateProtocolVersion(messagingVersion);
        pipeline.addLast("flushHandler", new FlushHandler(updatedParams));
        pipeline.addLast("messageOutHandler", new MessageOutHandler(updatedParams));
        pipeline.addLast("messageTimeoutHandler", new MessageOutTimeoutHandler(updatedParams));
        pipeline.addLast("errorLogger", new ErrorLoggingHandler());
    }

    private class ErrorLoggingHandler extends ChannelDuplexHandler
    {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (cause instanceof IOException)
                logger.trace("io error from {}", remoteAddr, cause);
            else
                logger.warn("error on channel from {}", remoteAddr, cause);

            ctx.close();
        }
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
