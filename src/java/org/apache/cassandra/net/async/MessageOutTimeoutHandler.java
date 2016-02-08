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

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;

/**
 * Checks each outbound message to see if it has timed out (by sitting in the channel for too long).
 */
class MessageOutTimeoutHandler extends ChannelOutboundHandlerAdapter
{
    // TODO:JEB double check Sylvain's comment about this one
    private final AtomicLong droppedMessageCount;

    MessageOutTimeoutHandler(OutboundConnectionParams params)
    {
        this.droppedMessageCount = params.droppedMessageCount;
    }

    @VisibleForTesting
    MessageOutTimeoutHandler(AtomicLong droppedMessageCount)
    {
        this.droppedMessageCount = droppedMessageCount;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        // optimize for the common case
        if (msg instanceof QueuedMessage)
        {
            if (!((QueuedMessage)msg).isTimedOut())
            {
                ctx.write(msg, promise);
            }
            else
            {
                droppedMessageCount.incrementAndGet();
                promise.setFailure(ExpiredException.INSTANCE);
            }
            return;
        }

        // if we got here, it's the wrong type of message
        promise.setFailure(new UnsupportedMessageTypeException("msg must be an instancce of " + QueuedMessage.class.getSimpleName()));
    }
}
