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

import java.util.EnumMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.transport.*;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public enum Option implements OptionCodec.Codecable<Option>
    {
        // The maximum number of rows to return in the first result frame
        MAX_RESULT_ROWS(1);

        private final int id;

        private Option(int id)
        {
            this.id = id;
        }

        public int getId()
        {
            return id;
        }

        public Object readValue(ChannelBuffer cb)
        {
            switch (this)
            {
                case MAX_RESULT_ROWS:
                    return cb.readInt();
                default:
                    throw new AssertionError();
            }
        }

        public void writeValue(Object value, ChannelBuffer cb)
        {
            switch (this)
            {
                case MAX_RESULT_ROWS:
                    assert value instanceof Integer;
                    cb.writeInt((Integer)value);
                    break;
            }
        }

        public int serializedValueSize(Object value)
        {
            switch (this)
            {
                case MAX_RESULT_ROWS:
                    return 4;
                default:
                    throw new AssertionError();
            }
        }
    }

    public static OptionCodec<Option> optionCodec = new OptionCodec<Option>(Option.class);

    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ChannelBuffer body)
        {
            String query = CBUtil.readLongString(body);
            Map<Option, Object> options = optionCodec.decode(body);
            return new QueryMessage(query, options);
        }

        public ChannelBuffer encode(QueryMessage msg)
        {
            ChannelBuffer qcb = CBUtil.longStringToCB(msg.query);
            ChannelBuffer ocb = optionCodec.encode(msg.options);
            return ChannelBuffers.wrappedBuffer(qcb, ocb);
        }
    };

    public final String query;
    public final Map<Option, Object> options;

    public QueryMessage(String query, Map<Option, Object> options)
    {
        super(Message.Type.QUERY);
        this.query = query;
        this.options = options;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute()
    {
        try
        {
            return QueryProcessor.process(query, connection.clientState());
        }
        catch (Exception e)
        {
            return new ErrorMessage(e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    @Override
    public String toString()
    {
        return "QUERY " + query + " (opts=" + options + ")";
    }
}
