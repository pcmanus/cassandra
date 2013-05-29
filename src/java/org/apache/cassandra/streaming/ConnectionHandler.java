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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.messages.StreamMessage;

/**
 * ConnectionHandler manages incoming/outgoing message exchange for the {@link StreamSession}.
 *
 * <p>
 * Internally, ConnectionHandler manages thread to receive incoming {@link StreamMessage} and thread to
 * send outgoing message. Messages are encoded/decoded on those thread and handed to
 * {@link StreamSession#messageReceived(org.apache.cassandra.streaming.messages.StreamMessage)}.
 */
public class ConnectionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private static final int MAX_CONNECT_ATTEMPTS = 3;

    private final StreamSession session;
    private IncomingMessageHandler incoming;
    private OutgoingMessageHandler outgoing;

    private boolean connected = false;
    private Socket socket;

    ConnectionHandler(StreamSession session)
    {
        this.session = session;
    }

    ConnectionHandler(StreamSession session, Socket socket)
    {
        this.session = session;
        this.socket = Preconditions.checkNotNull(socket);
        this.connected = socket.isConnected();
    }

    /**
     * Connect to peer and start exchanging message.
     * When connect attempt fails, this retries for maximum of MAX_CONNECT_ATTEMPTS times.
     *
     * @throws IOException when connection failed.
     */
    public void connect() throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                socket = MessagingService.instance().getConnectionPool(session.peer).newSocket();
                socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
                break;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt " + attempts + " to connect to " + session.peer + ". Retrying in " + waitms + " ms. (" + e + ")");
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new IOException("interrupted", wtf);
                }
            }
        }
        // send stream init message
        SocketChannel channel = socket.getChannel();
        WritableByteChannel out = channel;
        // socket channel is null when encrypted(SSL)
        if (channel == null)
        {
            out = Channels.newChannel(socket.getOutputStream());
        }
        logger.debug("Sending stream init...");
        out.write(session.createStreamInitMessage(MessagingService.current_version));

        connected = true;

        start();
        session.onConnect();
    }

    public void close()
    {
        incoming.terminate();
        outgoing.terminate();
        if (socket != null && !isConnected())
        {
            try
            {
                socket.close();
            }
            catch (IOException ignore) {}
        }
    }

    /**
     * Start incoming/outgoing messaging threads.
     */
    public void start() throws IOException
    {
        SocketChannel channel = socket.getChannel();
        ReadableByteChannel in = channel;
        WritableByteChannel out = channel;
        // socket channel is null when encrypted(SSL)
        if (channel == null)
        {
            in = Channels.newChannel(socket.getInputStream());
            out = Channels.newChannel(socket.getOutputStream());
        }

        incoming = new IncomingMessageHandler(session, in);
        outgoing = new OutgoingMessageHandler(session, out);

        // ready to send/receive files
        new Thread(incoming, "STREAM-IN-" + session.peer).start();
        new Thread(outgoing, "STREAM-OUT-" + session.peer).start();
    }

    public boolean isConnected()
    {
        return connected;
    }

    /**
     * Enqueue messages to be sent.
     *
     * @param messages messages to send
     */
    public void sendMessages(Collection<? extends StreamMessage> messages)
    {
        if (isConnected())
        {
            for (StreamMessage message : messages)
                sendMessage(message);
        }
    }

    public void sendMessage(StreamMessage message)
    {
        if (isConnected())
            outgoing.enqueue(message);
    }

    abstract static class MessageHandler implements Runnable
    {
        protected final StreamSession session;
        protected final AtomicBoolean terminated = new AtomicBoolean(false);

        protected MessageHandler(StreamSession session)
        {
            this.session = session;
        }

        public void terminate()
        {
            terminated.set(true);
        }

        public boolean terminated()
        {
            return terminated.get();
        }
    }

    /**
     * Incoming streaming message handler
     */
    static class IncomingMessageHandler extends MessageHandler
    {
        private final ReadableByteChannel in;

        IncomingMessageHandler(StreamSession session, ReadableByteChannel in)
        {
            super(session);
            this.in = in;
        }

        public void run()
        {
            while (!terminated())
            {
                try
                {
                    // receive message
                    StreamMessage message = StreamMessage.deserialize(in, session);
                    if (message != null)
                        session.messageReceived(message);
                }
                catch (SocketException e)
                {
                    // socket is closed
                    terminate();
                }
                catch (Throwable e)
                {
                    session.onError(e);
                }
            }
        }
    }

    /**
     * Outgoing file transfer thread
     */
    static class OutgoingMessageHandler extends MessageHandler
    {
        /*
         * All out going messages are queued up into messageQueue.
         * The size will grow when received streaming request.
         *
         * Queue is also PriorityQueue so that prior messages can go out fast.
         */
        private final PriorityBlockingQueue<StreamMessage> messageQueue = new PriorityBlockingQueue<>(64, new Comparator<StreamMessage>()
        {
            public int compare(StreamMessage o1, StreamMessage o2)
            {
                return o2.getPriority() - o1.getPriority();
            }
        });

        private final WritableByteChannel out;

        OutgoingMessageHandler(StreamSession session, WritableByteChannel out)
        {
            super(session);
            this.out = out;
        }

        public void enqueue(StreamMessage message)
        {
            messageQueue.put(message);
        }

        public void run()
        {
            while (!terminated())
            {
                try
                {
                    StreamMessage next = messageQueue.poll(1, TimeUnit.SECONDS);
                    if (next != null)
                    {
                        logger.debug("Sending " + next);
                        StreamMessage.serialize(next, out, session);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            terminate();
                    }
                    else if (terminated())
                    {
                        break;
                    }
                }
                catch (SocketException e)
                {
                    terminate();
                }
                catch (InterruptedException | IOException e)
                {
                    session.onError(e);
                }
            }
        }
    }
}
