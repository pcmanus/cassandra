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
package org.apache.cassandra.streaming.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;

public class StreamRequest
{
    public static final ISerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    public final Collection<Range<Token>> ranges;
    public final Collection<String> columnFamilies = new HashSet<>();

    public StreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies)
    {
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.columnFamilies.addAll(columnFamilies);
    }

    public static class StreamRequestSerializer implements ISerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutput out) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
                AbstractBounds.serializer.serialize(range, out, MessagingService.current_version);
            out.writeInt(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInput in) throws IOException
        {
            String keyspace = in.readUTF();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(in, MessagingService.current_version));
            int cfCount = in.readInt();
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, ranges, columnFamilies);
        }

        public long serializedSize(StreamRequest request, TypeSizes type)
        {
            int size = type.sizeof(request.keyspace);
            size += type.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
                size += AbstractBounds.serializer.serializedSize(range, MessagingService.current_version);
            size += type.sizeof(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                size += type.sizeof(cf);
            return size;
        }
    }
}
