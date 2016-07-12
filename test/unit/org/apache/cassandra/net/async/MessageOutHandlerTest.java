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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection.QueuedMessage;

public class MessageOutHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    @Test
    public void serializeMessage() throws IOException
    {
        MessageOutHandler handler = new MessageOutHandler(MESSAGING_VERSION, new AtomicLong(0), 8);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(1 <= channel.outboundMessages().size());
        channel.releaseOutbound();
        Assert.assertFalse(channel.finishAndReleaseAll());
    }
}
