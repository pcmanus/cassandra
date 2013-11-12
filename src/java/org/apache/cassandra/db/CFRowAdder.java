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
package org.apache.cassandra.db;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Convenience object to populate a given CQL3 row in a ColumnFamily object.
 *
 * This is meant for when performance is not of the utmost importance. When
 * performance matters, it might be worth allocating such builder.
 */
public class CFRowAdder
{
    public final ColumnFamily cf;
    public final Composite prefix;
    public final long timestamp;
    private final int ldt;

    public CFRowAdder(ColumnFamily cf, Composite prefix, long timestamp)
    {
        this.cf = cf;
        this.prefix = prefix;
        this.timestamp = timestamp;
        this.ldt = (int) (System.currentTimeMillis() / 1000);

        // If a CQL3 table, add the row marker
        if (cf.metadata().isCQL3Table())
            cf.addColumn(new Column(cf.getComparator().rowMarker(prefix), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp));
    }

    public CFRowAdder add(String cql3ColumnName, Object value)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        return add(cf.getComparator().create(prefix, def.name), def, value);
    }

    public CFRowAdder addMapEntry(String cql3ColumnName, Object key, Object value)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        assert def.type instanceof MapType;
        MapType mt = (MapType)def.type;
        CellName name = cf.getComparator().create(prefix, def.name, mt.keys.decompose(key));
        return add(name, def, value);
    }

    private ColumnDefinition getDefinition(String name)
    {
        return cf.metadata().getColumnDefinition(new ColumnIdentifier(name, false));
    }

    private CFRowAdder add(CellName name, ColumnDefinition def, Object value)
    {
        if (value == null)
            cf.addColumn(new DeletedColumn(name, ldt, timestamp));
        else
            cf.addColumn(new Column(name, ((AbstractType)def.type).decompose(value), timestamp));
        return this;
    }
}
