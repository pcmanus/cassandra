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

import java.util.EnumSet;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR        (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP      (1,  Direction.REQUEST,  StartupMessage.codec),
        READY        (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS  (4,  Direction.REQUEST,  CredentialsMessage.codec),
        OPTIONS      (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED    (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY        (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT       (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE      (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE      (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER     (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT        (12, Direction.RESPONSE, EventMessage.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        private Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }
    }

    public final Type type;
    protected volatile Connection connection;
    private volatile int streamId;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public abstract ByteBuf encode();

    public static abstract class Request extends Message
    {
        protected boolean tracingRequested;

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Response execute(QueryState queryState);

        public void setTracingRequested()
        {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested()
        {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message
    {
        protected UUID tracingId;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId()
        {
            return tracingId;
        }
    }

    @Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame, Message>
    {
        public Message decode(ChannelHandlerContext ctx, Frame frame)
        {
            boolean isRequest = frame.header.type.direction == Direction.REQUEST;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUuid(frame.body);

            Message message = frame.header.type.codec.decode(frame.body);
            message.setStreamId(frame.header.streamId);

            if (isRequest)
            {
                assert message instanceof Request;
                Request req = (Request)message;
                req.attach(frame.connection);
                if (isTracing)
                    req.setTracingRequested();
            }
            else
            {
                assert message instanceof Response;
                if (isTracing)
                    ((Response)message).setTracingId(tracingId);
            }

            return message;
        }
    }

    @Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message, Frame>
    {
        public Frame encode(ChannelHandlerContext ctx, Message message)
        {
            ByteBuf body = message.encode();
            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);
            if (message instanceof Response)
            {
                UUID tracingId = ((Response)message).getTracingId();
                if (tracingId != null)
                {
                    body = Unpooled.wrappedBuffer(CBUtil.uuidToCB(tracingId), body);
                    flags.add(Frame.Header.Flag.TRACING);
                }
            }
            else
            {
                assert message instanceof Request;
                if (((Request)message).isTracingRequested())
                    flags.add(Frame.Header.Flag.TRACING);
            }
            return Frame.create(message.type, message.getStreamId(), flags, body, message.connection());
        }
    }

    @Sharable
    public static class Dispatcher extends ChannelInboundMessageHandlerAdapter<Message>
    {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, Message message)
        {
            if (message instanceof Response)
                throw new ProtocolException("Invalid response message received, expecting requests");

            Request request = (Request)message;

            try
            {
                assert request.connection() instanceof ServerConnection;
                ServerConnection connection = (ServerConnection)request.connection();
                connection.validateNewMessage(request.type);

                logger.debug("Received: " + request);

                Response response = request.execute(connection.getQueryState(request.getStreamId()));
                response.setStreamId(request.getStreamId());
                response.attach(connection);
                connection.applyStateTransition(request.type, response.type);

                logger.debug("Responding: " + response);

                ctx.write(response);
            }
            catch (Exception ex)
            {
                // Don't let the exception propagate to exceptionCaught() if we can help it so that we can assign the right streamID.
                ctx.write(ErrorMessage.fromException(ex).setStreamId(request.getStreamId()));
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        throws Exception
        {
            if (ctx.channel().isOpen())
            {
                ChannelFuture future = ctx.write(ErrorMessage.fromException(cause));
                // On protocol exception, close the channel as soon as the message have been sent
                if (cause instanceof ProtocolException)
                {
                    future.addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) {
                            ctx.channel().close();
                        }
                    });
                }
            }
        }
    }
}
