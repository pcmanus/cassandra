package org.apache.cassandra.net.async;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.ConnectionUtils;
import org.apache.cassandra.net.async.InternodeMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A central spot for building Netty-based server and client Channels/Pipelines/Bootstrap instances.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    public enum Mode { MESSAGING, Mode, STREAMING }

    public static final String SSL_CHANNEL_HANDLER_NAME = "ssl";
    public static final String INBOUND_COMPRESSOR_HANDLER_NAME = "inboundCompressor";
    public static final String OUTBOUND_COMPRESSOR_HANDLER_NAME = "outboundCompressor";
    public static final String COALESCING_MESSAGE_CHANNEL_HANDLER_NAME = "messageCoalescer";
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
    private static final EventLoopGroup SERVER_GROUP = getEventLoopGroup(FBUtilities.getAvailableProcessors() * 4, "MessagingService-NettyServer-Threads");
    private static final EventLoopGroup CLIENT_GROUP = getEventLoopGroup(FBUtilities.getAvailableProcessors() * 4, "MessagingService-NettyClient-Threads");

    private static EventLoopGroup getEventLoopGroup(int threadCount, String threadNamePrefix)
    {
        return useEpoll
               ? new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix))
               : new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix));
    }

    private NettyFactory()
    {   }

    public static Channel createServerChannel(InetSocketAddress localAddr, ServerInitializer initializer) throws ConfigurationException
    {
        logger.info("Starting Netty-based Messaging Service on port {}", localAddr.getPort());
        Class<? extends ServerChannel> transport = useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        ServerBootstrap bootstrap = new ServerBootstrap().group(ACCEPT_GROUP, SERVER_GROUP)
                                                         .channel(transport)
                                                         .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                                                         .option(ChannelOption.SO_BACKLOG, 500)
                                                         .childOption(ChannelOption.SO_KEEPALIVE, true)
                                                         .childOption(ChannelOption.TCP_NODELAY, true)
                                                         .childOption(ChannelOption.SO_REUSEADDR, true)
                                                         .childHandler(initializer);

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

    public static class ServerInitializer extends ChannelInitializer<SocketChannel>
    {
        private final IInternodeAuthenticator authenticator;

        public ServerInitializer(IInternodeAuthenticator authenticator)
        {
            this.authenticator = authenticator;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast(HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME, new InboundHandshakeHandler(authenticator));
        }
    }

    public static class SecureServerInitializer extends ServerInitializer
    {
        private final ServerEncryptionOptions encryptionOptions;

        public SecureServerInitializer(IInternodeAuthenticator authenticator, ServerEncryptionOptions encryptionOptions)
        {
            super(authenticator);
            this.encryptionOptions = encryptionOptions;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();
            SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, true);
            pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslContext.newHandler(channel.alloc()));
            super.initChannel(channel);
        }
    }

    static Bootstrap createClientChannel(ClientChannelInitializer initializer, int sendBufferSize, boolean tcpNoDelay, int channelBufferSize)
    {
        Class<? extends Channel>  transport = useEpoll ? EpollSocketChannel.class : NioSocketChannel.class;
        Bootstrap bootstrap = new Bootstrap().group(CLIENT_GROUP)
                                             .channel(transport)
                                             .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                                             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                                             .option(ChannelOption.SO_KEEPALIVE, true)
                                             .option(ChannelOption.SO_REUSEADDR, true)
                                             .option(ChannelOption.SO_SNDBUF, sendBufferSize)
                                             .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                                             .handler(initializer);

        if (ConnectionUtils.isLocalDC(initializer.remoteAddr.getAddress()))
            bootstrap.option(ChannelOption.TCP_NODELAY, true);
        else
            bootstrap.option(ChannelOption.TCP_NODELAY, DatabaseDescriptor.getInterDCTcpNoDelay());

        if (DatabaseDescriptor.getInternodeSendBufferSize() != null)
            bootstrap.option(ChannelOption.SO_SNDBUF, DatabaseDescriptor.getInternodeSendBufferSize());

        return bootstrap;
    }

    static class ClientChannelInitializer extends ChannelInitializer<SocketChannel>
    {
        private final InetSocketAddress remoteAddr;
        private final int messagingVersion;
        private final boolean compress;
        private final Consumer<ConnectionHandshakeResult> callback;
        private final ServerEncryptionOptions encryptionOptions;
        private final Mode mode;

        ClientChannelInitializer(InetSocketAddress remoteAddr, int messagingVersion, boolean compress, Consumer<ConnectionHandshakeResult> callback, ServerEncryptionOptions encryptionOptions, Mode mode)
        {
            this.remoteAddr = remoteAddr;
            this.messagingVersion = messagingVersion;
            this.compress = compress;
            this.callback = callback;
            this.encryptionOptions = encryptionOptions;
            this.mode = mode;
        }

        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            if (NettyFactory.WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            if (encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, false);
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslContext.newHandler(channel.alloc()));
            }

            pipeline.addLast(HANDSHAKE_HANDLER_CHANNEL_HANDLER_NAME, new OutboundHandshakeHandler(remoteAddr, messagingVersion, compress, callback, mode));
        }
    }

    static InetAddress getInetAddress(Channel channel)
    {
        return ((InetSocketAddress)channel.remoteAddress()).getAddress();
    }

    public static boolean isSecure(Channel channel)
    {
        return channel.pipeline().get(SSL_CHANNEL_HANDLER_NAME) != null;
    }
}
