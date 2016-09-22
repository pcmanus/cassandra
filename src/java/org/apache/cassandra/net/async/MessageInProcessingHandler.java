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

import java.io.EOFException;
import java.io.IOException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

/**
 * Last {@link ChannelHandler} on the inbound side of internode messaging. The primary reason this class exists
 * is to centralize the channel error handling as well as abstracting the other handlers in the pipeline
 * from needing to directly invoke {@link MessagingService#receive(MessageIn, int)}.
 */
class MessageInProcessingHandler extends SimpleChannelInboundHandler<MessageInWrapper>
{
    private static final Logger logger = LoggerFactory.getLogger(MessageInProcessingHandler.class);
    static final Consumer<MessageInWrapper> MESSAGING_SERVICE_CONSUMER = msg -> MessagingService.instance().receive(msg.messageIn, msg.id);

    private final Consumer<MessageInWrapper> messageConsumer;

    MessageInProcessingHandler(Consumer<MessageInWrapper> messageConsumer)
    {
        this.messageConsumer = messageConsumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessageInWrapper msg) throws Exception
    {
        messageConsumer.accept(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        if (cause instanceof DecoderException)
            cause = cause.getCause();

        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownColumnFamilyException)
            logger.warn("UnknownColumnFamilyException reading from socket; closing", cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn(String.format("exception caught in inbound channel pipeline from %s", NettyFactory.getInetAddress(ctx.channel())), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        logger.info("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }
}
