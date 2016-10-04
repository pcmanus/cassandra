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
import java.util.Optional;
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
import io.netty.handler.flush.FlushConsolidationHandler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult.Result;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.async.NettyFactory.COALESCING_MESSAGE_CHANNEL_HANDLER_NAME;

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

    /**
     * As we use netty's {@link FlushConsolidationHandler}, we can configure a max number of flush() events before it actually
     * flushes; see {@link FlushConsolidationHandler}'s docs for full details.
     */
    // TODO make this configurable? yet more config :-/
    private static final int MAX_MESSAGES_BEFORE_FLUSH = 32;

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

    private volatile boolean isCancelled;

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

    // invoked when we get the response back from the peer, which should contain the second message of the internode
    // messaging handshake
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
        int peerMessagingVersion = in.readInt();

        Result result;
        if (messagingVersion > peerMessagingVersion)
        {
            logger.trace("Target max version is {}; will reconnect with that version", peerMessagingVersion);
            result = Result.DISCONNECT;
            // OTC.disconnect(); -- close socket, do *not* return any message
//            ctx.close();
        }
        else if (messagingVersion < peerMessagingVersion && messagingVersion < MessagingService.current_version)
        {
            logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done", peerMessagingVersion, messagingVersion);
            result = Result.DISCONNECT;

            // OTC.softCloseSocket() - enqueue CLOSE_SENTINEL, *and* send 3rd message of internode messaging protocol handshake
//            ctx.pipeline().close();
        }
        else
        {
            ByteBuf buf = ctx.alloc().buffer(Integer.BYTES + CompactEndpointSerializationHelper.serializedSize(localAddr.getAddress()));
            try
            {
                buf.writeInt(MessagingService.current_version);
                @SuppressWarnings("resource")
                DataOutput bbos = new ByteBufOutputStream(buf);
                CompactEndpointSerializationHelper.serialize(localAddr.getAddress(), bbos);
                ctx.writeAndFlush(buf);
                setupPipeline(ctx.channel().pipeline(), peerMessagingVersion);
                result = Result.GOOD;
            }
            catch (Exception e)
            {
                logger.info("failed to write last internode messaging handshake message", e);
                buf.release();
                result = Result.NEGOTIATION_FAILURE;
            }
        }

        callback.accept(new ConnectionHandshakeResult(ctx.channel(), peerMessagingVersion, result));
        ctx.channel().pipeline().remove(this);
    }

    void setupPipeline(ChannelPipeline pipeline, int messagingVersion)
    {
        if (params.compress)
            pipeline.addLast(NettyFactory.OUTBOUND_COMPRESSOR_HANDLER_NAME, new Lz4FrameEncoder());

        // only create an updated params instance if the messaging versions are different
        OutboundConnectionParams updatedParams = messagingVersion == params.protocolVersion ? params : params.updateProtocolVersion(messagingVersion);
        pipeline.addLast("flushConsolidator", new FlushConsolidationHandler(MAX_MESSAGES_BEFORE_FLUSH));
        pipeline.addLast("messageOutHandler", new MessageOutHandler(updatedParams));
        if (params.maybeCoalesce)
            coalescingStrategy().map(cs -> pipeline.addLast(COALESCING_MESSAGE_CHANNEL_HANDLER_NAME, new CoalescingMessageOutHandler(cs, params.droppedMessageCount)));
        pipeline.addLast("lameoLogger", new ErrorLoggingHandler());
    }

    private Optional<CoalescingStrategy> coalescingStrategy()
    {
        String strategyName = DatabaseDescriptor.getOtcCoalescingStrategy();
        String displayName = remoteAddr.getAddress().getHostAddress();
        CoalescingStrategy coalescingStrategy = CoalescingStrategies.newCoalescingStrategy(strategyName,
                                                                                           DatabaseDescriptor.getOtcCoalescingWindow(),
                                                                                           CoalescingMessageOutHandler.logger,
                                                                                           displayName);

        return coalescingStrategy.isCoalescing() ? Optional.of(coalescingStrategy) : Optional.empty();
    }

    private class ErrorLoggingHandler extends ChannelDuplexHandler
    {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (cause instanceof IOException)
                logger.debug("IOException from {}", remoteAddr, cause);
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
