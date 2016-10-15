package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A central spot for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    public enum Mode { MESSAGING, Mode, STREAMING }

    private static final String SSL_CHANNEL_HANDLER_NAME = "ssl";
    static final String INBOUND_COMPRESSOR_HANDLER_NAME = "inboundCompressor";
    static final String OUTBOUND_COMPRESSOR_HANDLER_NAME = "outboundCompressor";
    static final String COALESCING_MESSAGE_CHANNEL_HANDLER_NAME = "messageCoalescer";
    static final String HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME = "handshakeHandler";

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    private static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private static final boolean useEpoll = NativeTransportService.useEpoll();
    private static final EventLoopGroup ACCEPT_GROUP = getEventLoopGroup(FBUtilities.getAvailableProcessors(), "MessagingService-NettyAcceptor-Threads");
    private static final EventLoopGroup INBOUND_GROUP = getEventLoopGroup(FBUtilities.getAvailableProcessors() * 4, "MessagingService-NettyInbound-Threads");
    private static final EventLoopGroup OUTBOUND_GROUP = getEventLoopGroup(FBUtilities.getAvailableProcessors() * 4, "MessagingService-NettyOutbound-Threads");

    private static EventLoopGroup getEventLoopGroup(int threadCount, String threadNamePrefix)
    {
        return useEpoll
               ? new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix))
               : new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix));
    }

    private NettyFactory()
    {   }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    public static Channel createInboundChannel(InetSocketAddress localAddr, InboundInitializer initializer, int receiveBufferSize) throws ConfigurationException
    {
        logger.info("Starting Messaging Service on {}", localAddr);
        Class<? extends ServerChannel> transport = useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        ServerBootstrap bootstrap = new ServerBootstrap().group(ACCEPT_GROUP, INBOUND_GROUP)
                                                         .channel(transport)
                                                         .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                                                         .option(ChannelOption.SO_BACKLOG, 500)
                                                         .childOption(ChannelOption.SO_KEEPALIVE, true)
                                                         .childOption(ChannelOption.TCP_NODELAY, true)
                                                         .childOption(ChannelOption.SO_REUSEADDR, true)
                                                         .childHandler(initializer);

        if (receiveBufferSize > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBufferSize);

        ChannelFuture channelFuture = bootstrap.bind(localAddr);

        if (!channelFuture.awaitUninterruptibly().isSuccess())
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();

                Throwable failedChannelCause = channelFuture.cause();
            if (failedChannelCause.getMessage().contains("in use"))
                throw new ConfigurationException(channelFuture.channel().remoteAddress()
                                                 + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            else if (failedChannelCause.getMessage().contains("Cannot assign requested address"))
                throw new ConfigurationException("Unable to bind to address " + channelFuture.channel().remoteAddress()
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            else
                throw new IllegalStateException(failedChannelCause);
        }

        logger.info("Started MessagingService on IP [{}]", localAddr);
        return channelFuture.channel();
    }

    public static class InboundInitializer extends ChannelInitializer<SocketChannel>
    {
        private final IInternodeAuthenticator authenticator;
        private final ServerEncryptionOptions encryptionOptions;

        public InboundInitializer(IInternodeAuthenticator authenticator, ServerEncryptionOptions encryptionOptions)
        {
            this.authenticator = authenticator;
            this.encryptionOptions = encryptionOptions;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, true);
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslContext.newHandler(channel.alloc()));
            }

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast(HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME, new InboundHandshakeHandler(authenticator));
        }
    }

    /**
     * Create the {@link Bootstrap} for connecting to a remote peer. This method does <b>not</b> attempt to connect to the peer,
     * and thus does not block.
     */
    static Bootstrap createOutboundBootstrap(OutboundChannelInitializer initializer, int sendBufferSize, boolean tcpNoDelay)
    {
        Class<? extends Channel>  transport = useEpoll ? EpollSocketChannel.class : NioSocketChannel.class;
        return new Bootstrap().group(OUTBOUND_GROUP)
                                             .channel(transport)
                                             .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                                             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                                             .option(ChannelOption.SO_KEEPALIVE, true)
                                             .option(ChannelOption.SO_REUSEADDR, true)
                                             .option(ChannelOption.SO_SNDBUF, sendBufferSize)
                                             .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                                             .handler(initializer);
    }

    static class OutboundChannelInitializer extends ChannelInitializer<SocketChannel>
    {
        private final OutboundConnectionParams params;

        OutboundChannelInitializer(OutboundConnectionParams params)
        {
            this.params = params;
        }

        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (params.encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(params.encryptionOptions, true, false);
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslContext.newHandler(channel.alloc()));
            }

            if (NettyFactory.WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast(HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME, new OutboundHandshakeHandler(params));
        }
    }
}
