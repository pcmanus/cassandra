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
package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.*;

import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;

import org.junit.Test;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.Event.TopologyChange;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.StatusChange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Serialization/deserialization tests for protocol objects and messages.
 */
public class SerDeserTest
{
    @Test
    public void collectionSerDeserTest() throws Exception
    {
        collectionSerDeserTest(2);
        collectionSerDeserTest(3);
    }

    public void collectionSerDeserTest(int version) throws Exception
    {
        // Lists
        ListType<?> lt = ListType.getInstance(Int32Type.instance);
        List<Integer> l = Arrays.asList(2, 6, 1, 9);

        List<ByteBuffer> lb = new ArrayList<>(l.size());
        for (Integer i : l)
            lb.add(Int32Type.instance.decompose(i));

        assertEquals(l, lt.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(lb, lb.size(), version), version));

        // Sets
        SetType<?> st = SetType.getInstance(UTF8Type.instance);
        Set<String> s = new LinkedHashSet<>();
        s.addAll(Arrays.asList("bar", "foo", "zee"));

        List<ByteBuffer> sb = new ArrayList<>(s.size());
        for (String t : s)
            sb.add(UTF8Type.instance.decompose(t));

        assertEquals(s, st.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(sb, sb.size(), version), version));

        // Maps
        MapType<?, ?> mt = MapType.getInstance(UTF8Type.instance, LongType.instance);
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("bar", 12L);
        m.put("foo", 42L);
        m.put("zee", 14L);

        List<ByteBuffer> mb = new ArrayList<>(m.size() * 2);
        for (Map.Entry<String, Long> entry : m.entrySet())
        {
            mb.add(UTF8Type.instance.decompose(entry.getKey()));
            mb.add(LongType.instance.decompose(entry.getValue()));
        }

        assertEquals(m, mt.getSerializer().deserializeForNativeProtocol(CollectionSerializer.pack(mb, m.size(), version), version));
    }

    @Test
    public void eventSerDeserTest() throws Exception
    {
        eventSerDeserTest(2);
        eventSerDeserTest(3);
    }

    public void eventSerDeserTest(int version) throws Exception
    {
        List<Event> events = new ArrayList<>();

        events.add(TopologyChange.newNode(FBUtilities.getBroadcastAddress(), 42));
        events.add(TopologyChange.removedNode(FBUtilities.getBroadcastAddress(), 42));
        events.add(TopologyChange.movedNode(FBUtilities.getBroadcastAddress(), 42));

        events.add(StatusChange.nodeUp(FBUtilities.getBroadcastAddress(), 42));
        events.add(StatusChange.nodeDown(FBUtilities.getBroadcastAddress(), 42));

        events.add(new SchemaChange(SchemaChange.Change.CREATED, "ks"));
        events.add(new SchemaChange(SchemaChange.Change.UPDATED, "ks"));
        events.add(new SchemaChange(SchemaChange.Change.DROPPED, "ks"));

        events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TABLE, "ks", "table"));
        events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.TABLE, "ks", "table"));
        events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.TABLE, "ks", "table"));

        if (version >= 3)
        {
            events.add(new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TYPE, "ks", "type"));
            events.add(new SchemaChange(SchemaChange.Change.UPDATED, SchemaChange.Target.TYPE, "ks", "type"));
            events.add(new SchemaChange(SchemaChange.Change.DROPPED, SchemaChange.Target.TYPE, "ks", "type"));
        }

        for (Event ev : events)
        {
            ByteBuf buf = Unpooled.buffer(ev.serializedSize(version));
            ev.serialize(buf, version);
            assertEquals(ev, Event.deserialize(buf, version));
        }
    }
}
