package org.apache.cassandra.net.async;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

/**
 * 'Server'-side component that negotiates the internode handshake when establishing a new connection.
 * This handler will be the first in the netty channel for each incoming connection, and once the handshake is successful,
 * it will configure ther proper handlers for and remove itself from the working pipeline.
 */
class InboundHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    enum State { START, AWAITING_HANDSHAKE_BEGIN, AWAIT_STREAM_START_RESPONSE, AWAIT_MESSAGING_START_RESPONSE, MESSAGING_HANDSHAKE_COMPLETE, HANDSHAKE_FAIL }

    private State state;

    private final IInternodeAuthenticator authenticator;
    private boolean hasAuthenticated;

    /**
     * The peer's declared messaging version.
     */
    private int version;

    /**
     * Does the peer support (or want to use) compressed data?
     */
    private boolean compressed;

    /**
     * A future the essentially places a timeout on how long we'll wait for the peer
     * to complete the next step of the handshake.
     */
    private Future<?> handshakeTimeout;

    InboundHandshakeHandler(IInternodeAuthenticator authenticator)
    {
        this.authenticator = authenticator;
        state = State.START;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        if (!hasAuthenticated && !handleAuthenticate(ctx.channel().remoteAddress(), ctx))
            return;

        try
        {
            switch (state)
            {
                case START:
                    state = handleStart(ctx, in);
                    break;
                case AWAIT_MESSAGING_START_RESPONSE:
                    state = handleMessagingStartResponse(ctx, in);
                    break;
                case HANDSHAKE_FAIL:
                    throw new IllegalStateException("channel should be closed after determining the handshake failed with peer: " + ctx.channel().remoteAddress());
                default:
                    logger.error("unhandled state: " + state);
                    state = State.HANDSHAKE_FAIL;
                    ctx.close();
            }
        }
        catch (Exception e)
        {
            logger.error("unexpected error while negotiating internode messaging handshake", e);
            state = State.HANDSHAKE_FAIL;
            ctx.close();
        }
    }

    /**
     * Ensure the peer is allowed to connect to this node.
     */
    @VisibleForTesting
    boolean handleAuthenticate(SocketAddress socketAddress, ChannelHandlerContext ctx)
    {
        // the only reason addr would not be instanceof InetSocketAddress is in unit testing, when netty's EmbeddedChannel
        // uses EmbeddedSocketAddress. Normally, we'd do an instanceof for that class name, but it's marked with default visibility,
        // so we can't reference it outside of it's package (and so it doesn't compile).
        if (socketAddress instanceof InetSocketAddress)
        {
            InetSocketAddress addr = (InetSocketAddress)socketAddress;
            if (!authenticator.authenticate(addr.getAddress(), addr.getPort()))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Failed to authenticate peer {}", addr);
                ctx.close();
                return false;
            }
        }
        hasAuthenticated = true;
        return true;
    }

    /**
     * Handles receiving the first message in the internode messaging handshake protocol. If the sender's protocol version
     * is accepted, we respond with the second message of the handshake protocol.
     */
    @VisibleForTesting
    State handleStart(ChannelHandlerContext ctx, ByteBuf in) throws IOException
    {
        if (in.readableBytes() < OutboundHandshakeHandler.FIRST_MESSAGE_LENGTH)
            return State.START;

        MessagingService.validateMagic(in.readInt());
        int header = in.readInt();
        version = MessagingService.getBits(header, 15, 8);

        boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
        if (isStream)
        {
            // TODO fill in once streaming is moved to netty
            ctx.close();
            return State.AWAIT_STREAM_START_RESPONSE;
        }
        else
        {
            if (version < MessagingService.VERSION_30)
            {
                logger.error("Unable to read obsolete message version {} from {}; The earliest version supported is 3.0.0", version, ctx.channel().remoteAddress());
                ctx.close();
                return State.HANDSHAKE_FAIL;
            }

            logger.trace("Connection version {} from {}", version, ctx.channel().remoteAddress());
            compressed = MessagingService.getBits(header, 2, 1) == 1;

            // if this version is < the MS version the other node is trying
            // to connect with, the other node will disconnect
            ByteBuf outBuf = ctx.alloc().buffer(OutboundHandshakeHandler.SECOND_MESSAGE_LENGTH);
            outBuf.writeInt(MessagingService.current_version);
            ctx.writeAndFlush(outBuf).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

            // outbound side will reconnect to change the version
            if (version > MessagingService.current_version)
            {
                logger.error("peer wants to use a messaging version higher ({}) than what this node supports ({})", version, MessagingService.current_version);
                ctx.close();
                return State.HANDSHAKE_FAIL;
            }

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
            handshakeTimeout = ctx.executor().schedule(() -> failHandshake(ctx), timeout, TimeUnit.MILLISECONDS);
            return State.AWAIT_MESSAGING_START_RESPONSE;
        }
    }

    /**
     * Handles the third (and last) message in the internode messaging handshake protocol. Grabs the protocol version and
     * IP addr the peer wants to use.
     */
    @VisibleForTesting
    State handleMessagingStartResponse(ChannelHandlerContext ctx, ByteBuf in) throws IOException
    {
        if (in.readableBytes() < OutboundHandshakeHandler.THIRD_MESSAGE_LENGTH_MIN)
            return State.AWAIT_MESSAGING_START_RESPONSE;

        if (handshakeTimeout != null)
        {
            handshakeTimeout.cancel(false);
            handshakeTimeout = null;
        }

        int maxVersion = in.readInt();
        if (maxVersion > MessagingService.current_version)
        {
            logger.error("peer wants to use a messaging version higher ({}) than what this node supports ({})", maxVersion, MessagingService.current_version);
            ctx.close();
            return State.HANDSHAKE_FAIL;
        }

        // record the (true) version of the endpoint
        @SuppressWarnings("resource")
        DataInput inputStream = new ByteBufInputStream(in);
        final InetAddress from = CompactEndpointSerializationHelper.deserialize(inputStream);
        MessagingService.instance().setVersion(from, maxVersion);
        logger.trace("Set version for {} to {} (will use {})", from, maxVersion, MessagingService.instance().getVersion(from));

        setupMessagingPipeline(ctx.pipeline(), from, compressed, version);
        return State.MESSAGING_HANDSHAKE_COMPLETE;
    }

    @VisibleForTesting
    void setupMessagingPipeline(ChannelPipeline pipeline, InetAddress peer, boolean compressed, int messagingVersion)
    {
        if (compressed)
            pipeline.addLast(NettyFactory.INBOUND_COMPRESSOR_HANDLER_NAME, new Lz4FrameDecoder());

        pipeline.addLast("messageInHandler", new MessageInHandler(peer, messagingVersion));
        pipeline.remove(this);
    }

    private void failHandshake(ChannelHandlerContext ctx)
    {
        if (state == State.MESSAGING_HANDSHAKE_COMPLETE)
            return;

        state = State.HANDSHAKE_FAIL;
        ctx.close();

        if (handshakeTimeout != null)
            handshakeTimeout.cancel(false);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("Failed to properly handshake with peer {}. Closing the channel.", ctx.channel().remoteAddress());
        failHandshake(ctx);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("Failed to properly handshake with peer {}. Closing the channel.", ctx.channel().remoteAddress(), cause);
        failHandshake(ctx);
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }
}
