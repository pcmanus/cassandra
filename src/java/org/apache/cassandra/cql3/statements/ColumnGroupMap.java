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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.CollectionType;

public class ColumnGroupMap
{
    private final Map<ByteBuffer, Value> map = new HashMap<ByteBuffer, Value>();

    public void add(ByteBuffer[] fullName, int groupIdx, IColumn column)
    {
        ByteBuffer columnName = fullName[groupIdx];
        if (fullName.length == groupIdx + 2)
        {
            // It's a collection
            Value v = map.get(columnName);
            if (v == null)
            {
                v = new Collection();
                map.put(columnName, v);
            }
            assert v instanceof Collection;

            ((Collection)v).put(fullName[groupIdx + 1], column);
        }
        else
        {
            assert !map.containsKey(columnName);
            map.put(columnName, new Simple(column));
        }
    }

    public IColumn getSimple(ByteBuffer key)
    {
        Value v = map.get(key);
        if (v == null)
            return null;

        assert v instanceof Simple;
        return ((Simple)v).column;
    }

    public ByteBuffer getCollectionForThrift(CollectionType t, ByteBuffer key)
    {
        Value v = map.get(key);
        if (v == null)
            return null;

        assert v instanceof Collection;
        return t.serializeForThrift((Map<ByteBuffer, IColumn>)v);
    }

    private interface Value {};

    private static class Simple implements Value
    {
        public final IColumn column;

        Simple(IColumn column)
        {
            this.column = column;
        }
    }

    private static class Collection extends LinkedHashMap<ByteBuffer, IColumn> implements Value {}
}
