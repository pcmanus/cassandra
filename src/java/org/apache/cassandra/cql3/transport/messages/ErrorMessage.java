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
package org.apache.cassandra.cql3.transport.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.transport.CBUtil;
import org.apache.cassandra.cql3.transport.Message;

/**
 * Message to indicate an error to the client.
 */
public class ErrorMessage extends Message.Response
{
    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>()
    {
        public ErrorMessage decode(ChannelBuffer body)
        {
            int code = body.readInt();
            String msg = CBUtil.readString(body);
            return new ErrorMessage(msg);
        }

        public ChannelBuffer encode(ErrorMessage msg)
        {
            ChannelBuffer ccb = CBUtil.intToCB(msg.code);
            ChannelBuffer mcb = CBUtil.stringToCB(msg.errorMsg);
            return ChannelBuffers.wrappedBuffer(ccb, mcb);
        }
    };

    // We need to figure error codes out (#3979)
    public final int code;
    public final String errorMsg;

    public ErrorMessage(int code, String errorMsg)
    {
        super(Message.Type.ERROR);
        this.code = code;
        this.errorMsg = errorMsg;
    }

    public ErrorMessage(String errorMsg)
    {
        // Assigning error 0 to everyone for now
        this(0, errorMsg);
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "ERROR " + code + ": " + errorMsg;
    }
}
