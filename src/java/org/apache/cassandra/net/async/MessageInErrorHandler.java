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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.apache.cassandra.exceptions.UnknownTableException;

/**
 * Last {@link ChannelHandler} on the inbound side of internode messaging. The reason this class exists
 * is to centralize the inbound channel error handling.
 */
class MessageInErrorHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(MessageInErrorHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        // if there's a problem deserializing an incoming message, the exception is thrown up to netty, which wraps it in a DecoderException.
        if (cause instanceof DecoderException && cause.getCause() != null)
            cause = cause.getCause();

        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("UnknownColumnFamilyException reading from socket; closing", cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("exception caught in inbound channel pipeline from " + ctx.channel().remoteAddress(), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }
}
