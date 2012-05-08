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
package org.apache.cassandra.cql3.transport;

import java.util.*;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Hex;

/**
 * A handler for debugging the protocol.
 * It logs whatever bytes are received and sent (in hexadecimal).
 */
public class DebugHandler extends SimpleChannelHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DebugHandler.class);

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
        ChannelBuffer cb = (ChannelBuffer)e.getMessage();
        logger.info("Received " + toHex(cb));
        super.messageReceived(ctx, e);
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
        ChannelBuffer cb = (ChannelBuffer)e.getMessage();
        logger.info("Writting " + toHex(cb));
        super.writeRequested(ctx, e);
    }

    public static String toHex(ChannelBuffer cb)
    {
        byte[] bytes = new byte[cb.readableBytes()];
        cb.duplicate().readBytes(bytes);
        String hex = Hex.bytesToHex(bytes);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hex.length(); ++i)
        {
            sb.append(hex.charAt(i));
            if (i <= hex.length() - 1)
                sb.append(hex.charAt(++i));
            sb.append(" ");
        }
        return sb.toString();
    }
}
