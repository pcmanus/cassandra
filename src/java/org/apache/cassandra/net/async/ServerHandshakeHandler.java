package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.net.async.NettyFactory.HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME;

/**
 * 'Server'-side component that negotiates the internode handshake when establishing a new connection.
 * This handler will be the first in the netty channel for each incoming connection, and once the handshake is successful,
 * it will configure ther proper handlers for and remove itself from the working pipeline.
 */
class ServerHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    private static final int FIRST_MESSAGE_LENGTH = 8;


    /**
     * The length of the third message in the internode message handshake protocol. We need to receive an int (version)
     * and an IP addr. If IPv4, that's 5 more bytes; if IPv6, it's 17 more bytes. Since we can't know apriori if the IP address
     * will be v4 or v6, go with the minimum requires bytes (5), and hope that if the address is v6, we'll have the extra 12 bytes in the packet.
     */
    private static final int THIRD_MESSAGE_LENGTH_MIN = 9;

    enum State { START, AWAITING_HANDSHAKE_BEGIN, AWAIT_STREAM_START_RESPONSE, AWAIT_MESSAGING_START_RESPONSE, MESSAGING_HANDSHAKE_COMPLETE, HANDSHAKE_FAIL }

    private State state;

    // not final as we need to inject a handler name for tests
    static String handshakeHandlerChannelHandlerName = HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME;

    private final IInternodeAuthenticator authenticator;
    private boolean hasAuthenticated;

    /**
     * The client's declared messaging version.
     */
    private int version;

    /**
     * Does the client support (or want to use) compressed data?
     */
    private boolean compressed;

    /**
     * A future the essentially places a timeout on how long we'll wait for the client
     * to complete the next step of the handshake.
     */
    private Future<?> handshakeResponse;

    ServerHandshakeHandler(IInternodeAuthenticator authenticator)
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
                    throw new IllegalStateException("channel should be closed after determining the handshake failed");
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
        if (in.readableBytes() < FIRST_MESSAGE_LENGTH)
            return State.START;

        MessagingService.validateMagic(in.readInt());
        int header = in.readInt();
        version = MessagingService.getBits(header, 15, 8);

        // these are two older checks, located in different spots in the MS/ITC code. hence, the difference styles
        // outbound side will reconnect if necessary to upgrade version
        if (version > MessagingService.current_version)
        {
            logger.error("peer wants to use a messaging version higher ({}) than what this node supports ({})", version, MessagingService.current_version);
            ctx.close();
            return State.HANDSHAKE_FAIL;
        }

        if (version < MessagingService.VERSION_20)
        {
            logger.error("Unable to read obsolete message version {}; The earliest version supported is 2.0.0", version);
            ctx.close();
            return State.HANDSHAKE_FAIL;
        }

        logger.trace("Connection version {} from {}", version, ctx.channel().remoteAddress());

        compressed = MessagingService.getBits(header, 2, 1) == 1;
        boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
        if (isStream)
        {
            // TODO fill in once streaming is moved to netty
            return State.AWAIT_STREAM_START_RESPONSE;
        }
        else
        {
            // if this version is < the MS version the other node is trying
            // to connect with, the other node will disconnect
            ByteBuf outBuf = ctx.alloc().ioBuffer(4);
            outBuf.writeInt(MessagingService.current_version);

            ctx.writeAndFlush(outBuf).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
            handshakeResponse = ctx.executor().schedule((Runnable) ctx::close, timeout, TimeUnit.MILLISECONDS);
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
        if (in.readableBytes() < THIRD_MESSAGE_LENGTH_MIN)
            return State.AWAIT_MESSAGING_START_RESPONSE;

        if (handshakeResponse != null)
        {
            handshakeResponse.cancel(false);
            handshakeResponse = null;
        }

        int maxVersion = in.readInt();
        if (maxVersion > MessagingService.current_version)
        {
            logger.error("peer wants to use a messaging version higher ({}) than what this node supports ({})", maxVersion, MessagingService.current_version);
            ctx.close();
            return State.HANDSHAKE_FAIL;
        }

        // record the (true) version of the endpoint
        final InetAddress from;
        try (ByteBufInputStream bbis = new ByteBufInputStream(in))
        {
            from = CompactEndpointSerializationHelper.deserialize(bbis);
            MessagingService.instance().setVersion(from, maxVersion);
            logger.trace("Set version for {} to {} (will use {})", from, maxVersion, MessagingService.instance().getVersion(from));
        }

        setupMessagingPipeline(ctx.pipeline(), createHandlers(from, compressed, version, MessageInProcessingHandler.MESSAGING_SERVICE_CONSUMER));
        return State.MESSAGING_HANDSHAKE_COMPLETE;
    }

    /**
     * Creates the list of {@link ChannelHandler}s to service the client.
     */
    static List<Pair<String, ChannelHandler>> createHandlers(InetAddress peer, boolean compressed, int messagingVersion, Consumer<MessageInWrapper> messageConsumer)
    {
        List<Pair<String, ChannelHandler>> namesToHandlers;
        if (messagingVersion >= MessagingService.VERSION_40)
        {
            namesToHandlers = new ArrayList<>(4);
            if (compressed)
                namesToHandlers.add(Pair.create(NettyFactory.INBOUND_COMPRESSOR_HANDLER_NAME, new Lz4FrameDecoder()));

            final int maxSize = 1 << 27; // 128MB as the largest frame (?)
            namesToHandlers.add(Pair.create("frameDecoder", new LengthFieldBasedFrameDecoder(maxSize, 4, 4)));
            namesToHandlers.add(Pair.create("messageInHandler", new MessageInHandler(peer, messagingVersion)));
            namesToHandlers.add(Pair.create("messageInProcessor", new MessageInProcessingHandler(messageConsumer)));
        }
        else
        {
            namesToHandlers = Collections.singletonList(Pair.create("messageInHandler", new LegacyClientHandler(peer, compressed, messagingVersion)));
        }

        return namesToHandlers;
    }

    /**
     * Add the {@link ChannelHandler}s to the pipeline. We can't naively just shove the handlers onto the end of the pipeline
     * (via {@link ChannelPipeline#addLast(String, ChannelHandler)}) due to unit testing. {@link EmbeddedChannel} adds it's own
     * handler at the end of the channel, which collects messages and does not send the message to any further handlers in the pipeline.
     * Hence, we have to add new handlers after the current {@link ServerHandshakeHandler}.
     */
    @VisibleForTesting
    static void setupMessagingPipeline(ChannelPipeline pipeline, List<Pair<String, ChannelHandler>> namesToHandlers)
    {
        String after = handshakeHandlerChannelHandlerName;
        for (Pair<String, ChannelHandler> pair : namesToHandlers)
        {
            pipeline.addAfter(after, pair.left, pair.right);
            after = pair.left;
        }

        pipeline.remove(handshakeHandlerChannelHandlerName);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("Failed to properly handshake with peer {}. Closing the channel.", ctx.channel().remoteAddress(), cause);
        state = State.HANDSHAKE_FAIL;
        ctx.close();
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }
}
