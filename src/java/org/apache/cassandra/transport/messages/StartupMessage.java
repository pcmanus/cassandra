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

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends Message.Request
{
    public static final String CQL_VERSION = "CQL_VERSION";
    public static final String COMPRESSION = "COMPRESSION";

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ByteBuf body)
        {
            return new StartupMessage(CBUtil.readStringMap(body));
        }

        public ByteBuf encode(StartupMessage msg)
        {
            ByteBuf cb = Unpooled.buffer();
            CBUtil.writeStringMap(cb, msg.options);
            return cb;
        }
    };

    public final Map<String, String> options;

    public StartupMessage(Map<String, String> options)
    {
        super(Message.Type.STARTUP);
        this.options = options;
    }

    public ByteBuf encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute(QueryState state)
    {
        ClientState cState = state.getClientState();
        String cqlVersion = options.get(CQL_VERSION);
        if (cqlVersion == null)
            throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");

        try 
        {
            cState.setCQLVersion(cqlVersion);
        }
        catch (InvalidRequestException e)
        {
            throw new ProtocolException(e.getMessage());
        }

        if (cState.getCQLVersion().compareTo(new SemanticVersion("2.99.0")) < 0)
            throw new ProtocolException(String.format("CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)", cqlVersion));

        if (options.containsKey(COMPRESSION))
        {
            String compression = options.get(COMPRESSION).toLowerCase();
            if (compression.equals("snappy"))
            {
                if (FrameCompressor.SnappyCompressor.instance == null)
                    throw new ProtocolException("This instance does not support Snappy compression");
                connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
            }
            else
            {
                throw new ProtocolException(String.format("Unknown compression algorithm: %s", compression));
            }
        }

        if (cState.isLogged())
            return new ReadyMessage();
        else
            return new AuthenticateMessage(DatabaseDescriptor.getAuthenticator().getClass().getName());
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }
}
