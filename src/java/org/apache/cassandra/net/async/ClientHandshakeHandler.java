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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult.Result;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link ChannelHandler} to execute the client-side of the internode communication handshake protocol.
 * As soon as the handler is added to the channel via {@code #channelActive} (which is only invoked if the underlying connection
 * was properly established), the first message of the internode messaging protocol is automatically sent out.
 *
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because after the first message is sent
 * on becoming active in the channel, it waits for the peer's response (the second message of the internode messaging handshake protocol).
 */
class ClientHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(ClientHandshakeHandler.class);

    /**
     * The length of the second message of the internode messaging handshake.
     */
    private static final int SECOND_MESSAGE_LENGTH = 4;

    /**
     * The address to tell the peer we are. Passing in and memoizing this value eliminates a dependency on {@link FBUtilities#getBroadcastAddress()}.
     */
    private final InetSocketAddress remoteAddr;

    /**
     * The expected messaging service version to use.
     */
    private final int messagingVersion;

    /**
     * Declares if transferred data should be compressed.
     */
    private final boolean compress;

    /**
     * A function to invoke upon completion of the attempt, success or failure, to connect to the peer.
     */
    private final Consumer<ConnectionHandshakeResult> callback;
    private final NettyFactory.Mode mode;

    /**
     * A future that places a timeout on how long we'll wait for the server to respond
     * so we can move on to the next step of the handshake or just fail.
     */
    private Future<?> handshakeResponse;

    private volatile boolean isCancelled;

    ClientHandshakeHandler(InetSocketAddress remoteAddr, int messagingVersion, boolean compress, Consumer<ConnectionHandshakeResult> callback, NettyFactory.Mode mode)
    {
        this.remoteAddr = remoteAddr;
        this.messagingVersion = messagingVersion;
        this.compress = compress;
        this.callback = callback;
        this.mode = mode;
    }

    // invoked when the channel is active, and sends out the first handshake message
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        ctx.writeAndFlush(firstHandshakeMessage(ctx.alloc().ioBuffer(8), createHeader(messagingVersion, compress, mode)));
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        handshakeResponse = ctx.executor().schedule(() -> handshakeTimeout(ctx), timeout, TimeUnit.MILLISECONDS);
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
        // 2 bits: unused.  used to be "serializer type," which was always Binary
        // 1 bit: compression
        // 1 bit: streaming mode
        // 2 bits: unused
        // 8 bits: version
        // 15 bits: unused

        int header = 0;
        if (compressionEnabled)
            header |= 1 << 2;
        if (mode == NettyFactory.Mode.STREAMING)
            header |= 8;

        header |= (version << 8);
        return header;
    }

    // invoked when we get the response back from the server, which should contain the second message of the internode
    // messaging handshake
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        if (isCancelled)
            return;

        if (in.readableBytes() < SECOND_MESSAGE_LENGTH)
            return;

        if (handshakeResponse != null)
        {
            handshakeResponse.cancel(false);
            handshakeResponse = null;
        }
        int peerMessagingVersion = in.readInt();

        Result result;
        if (messagingVersion > peerMessagingVersion)
        {
            if (logger.isTraceEnabled())
                logger.trace("Target max version is {}; will reconnect with that version", peerMessagingVersion);
            result = Result.DISCONNECT;
        }
        else if (messagingVersion < peerMessagingVersion && messagingVersion < MessagingService.current_version)
        {
            if (logger.isTraceEnabled())
                logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done", peerMessagingVersion, messagingVersion);
            result = Result.DISCONNECT;
        }
        else
        {
            ByteBuf buf = ctx.alloc().ioBuffer(4 + CompactEndpointSerializationHelper.serializedSize(remoteAddr.getAddress()));
            try
            {
                buf.writeInt(MessagingService.current_version);
                @SuppressWarnings("resource")
                DataOutput bbos = new ByteBufOutputStream(buf);
                CompactEndpointSerializationHelper.serialize(remoteAddr.getAddress(), bbos);
                ctx.writeAndFlush(buf);
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

    /**
     * Handles the timeout for receiving a response to the handshake request.
     *
     * Note: This will happen on the netty IO thread, so there no races with {@link #decode(ChannelHandlerContext, ByteBuf, List)}.
     */
    private void handshakeTimeout(ChannelHandlerContext ctx)
    {
        if (isCancelled)
            return;

        isCancelled = true;
        ctx.close();
        if (callback != null)
            callback.accept(ConnectionHandshakeResult.failed());

        if (handshakeResponse != null)
            handshakeResponse.cancel(false);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        handshakeTimeout(ctx);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("exception in negotiating internode handshake", cause);
        handshakeTimeout(ctx);
        ctx.fireExceptionCaught(cause);
    }

    @VisibleForTesting
    boolean isCancelled()
    {
        return isCancelled;
    }
}