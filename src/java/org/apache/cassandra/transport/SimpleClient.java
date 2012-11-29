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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.logging.InternalLoggerFactory;
import io.netty.logging.Slf4JLoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.transport.messages.*;

public class SimpleClient
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final String host;
    public final int port;

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected final Connection connection = new Connection(tracker);
    protected Bootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Connection.Tracker tracker)
        {
            return connection;
        }
    };

    public SimpleClient(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public void connect(boolean useCompression) throws IOException
    {
        establishConnection();

        Map<String, String> options = new HashMap<String, String>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        if (useCompression)
        {
            options.put(StartupMessage.COMPRESSION, "snappy");
            connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
        }
        execute(new StartupMessage(options));
    }

    protected void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup())
                 .channel(NioSocketChannel.class)
                 .remoteAddress(host, port)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ClientInitializer());

        // Make a new connection.
        ChannelFuture future = bootstrap.connect();

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess())
        {
            bootstrap.shutdown();
            throw new IOException("Connection Error", future.cause());
        }
    }

    public void login(Map<String, String> credentials)
    {
        CredentialsMessage msg = new CredentialsMessage();
        msg.credentials.putAll(credentials);
        execute(msg);
    }

    public ResultMessage execute(String query, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new QueryMessage(query, consistency));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public ResultMessage.Prepared prepare(String query)
    {
        Message.Response msg = execute(new PrepareMessage(query));
        assert msg instanceof ResultMessage.Prepared;
        return (ResultMessage.Prepared)msg;
    }

    public ResultMessage executePrepared(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new ExecuteMessage(statementId, values, consistency));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public void close()
    {
        // Wait until all messages are flushed before closing the channel.
        if (lastWriteFuture != null)
            lastWriteFuture.awaitUninterruptibly();

        // Close the connection.  Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        channel.close().awaitUninterruptibly();

        // Shut down all thread pools to exit.
        bootstrap.shutdown();
    }

    protected Message.Response execute(Message.Request request)
    {
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.write(request);
            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    // Stateless handlers
    private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
    private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
    private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
    private static final Frame.Compressor frameCompressor = new Frame.Compressor();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}
        public void closeAll() {}
    }

    private class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        @Override
        public void initChannel(SocketChannel ch) throws Exception
        {
            ChannelPipeline pipeline = ch.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(tracker, connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);
        }
    }

    @Sharable
    private static class ResponseHandler extends ChannelInboundMessageHandlerAdapter<Message.Response>
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        @Override
        public void messageReceived(ChannelHandlerContext ctx, Message.Response response)
        {
            try
            {
                responses.put(response);
            }
            catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }
        }
    }
}
