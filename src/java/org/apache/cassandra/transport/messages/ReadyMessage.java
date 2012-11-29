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
package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class ReadyMessage extends Message.Response
{
    public static final Message.Codec<ReadyMessage> codec = new Message.Codec<ReadyMessage>()
    {
        public ReadyMessage decode(ByteBuf body)
        {
            return new ReadyMessage();
        }

        public ByteBuf encode(ReadyMessage msg)
        {
            return Unpooled.EMPTY_BUFFER;
        }
    };

    public ReadyMessage()
    {
        super(Message.Type.READY);
    }

    public ByteBuf encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "READY";
    }
}
