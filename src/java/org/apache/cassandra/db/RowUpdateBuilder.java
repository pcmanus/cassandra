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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Convenience object to create row updates.
 *
 * This is meant for system table update, when performance is not of the utmost importance.
 */
public class RowUpdateBuilder
{
    private final CFMetaData metadata;
    private final long timestamp;
    private final int ldt;

    private ClusteringPrefix clustering;
    private boolean isRowDeletion;

    private final SortedMap<ColumnDefinition, Set<Cell>> cells = new TreeMap<>();
    private final Set<ColumnDefinition> collectionsToReset = new HashSet<>();

    public RowUpdateBuilder(CFMetaData metadata, long timestamp)
    {
        this.metadata = metadata;
        this.timestamp = timestamp;
        this.ldt = (int) (System.currentTimeMillis() / 1000);
    }

    public RowUpdateBuilder clustering(Object... values)
    {
        clustering = metadata.comparator.make(values);
        return this;
    }

    public RowUpdateBuilder deleteRow()
    {
        this.isRowDeletion = true;
        return this;
    }

    public RowUpdateBuilder resetCollection(String columnName)
    {
        collectionsToReset.add(getDefinition(columnName));
        return this;
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        return addCell(columnName, value == null
                                   ? Cells.createTombsone(ldt, timestamp)
                                   : Cells.create(bb(value, getDefinition(columnName).type), timestamp, 0, metadata));
    }

    private ByteBuffer bb(Object value, AbstractType<?> type)
    {
        return (value instanceof ByteBuffer) ? (ByteBuffer)value : ((AbstractType)type).decompose(value);
    }

    public RowUpdateBuilder addMapEntry(String columnName, Object key, Object value)
    {
        ColumnDefinition def = getDefinition(columnName);
        assert def.type instanceof MapType;
        MapType mt = (MapType)def.type;
        return addCell(columnName, Cells.create(new CollectionPath(bb(key, mt.keys)), bb(value, mt.values), timestamp, 0, metadata));
    }

    public RowUpdateBuilder addListEntry(String columnName, Object value)
    {
        ColumnDefinition def = getDefinition(columnName);
        assert def.type instanceof ListType;
        ListType lt = (ListType)def.type;
        return addCell(columnName, Cells.create(new CollectionPath(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), bb(value, lt.elements), timestamp, 0, metadata));
    }

    private RowUpdateBuilder addCell(String name, Cell cell)
    {
        ColumnDefinition c = getDefinition(name);
        Set<Cell> existing = cells.get(c);
        if (existing != null)
        {
            existing.add(cell);
            return this;
        }

        if (c.isComplex())
        {
            existing = new HashSet<>();
            existing.add(cell);
            cells.put(c, existing);
        }
        else
        {
            cells.put(c, Collections.singleton(cell));
        }
        return this;
    }

    private ColumnDefinition getDefinition(String name)
    {
        return metadata.getColumnDefinition(new ColumnIdentifier(name, false));
    }

    public Mutation buildAndAddTo(Mutation mutation)
    {
        // TODO
        // We should probably always use all columns for the table, but it's wasted when we do a
        // deletion. So we might want to separate deletions from this object. But we might also
        // want to check when this is used and pass the builder directly instead of the Mutation?

        throw new UnsupportedOperationException();
        //PartitionUpdate update = mutation.addOrGet(metadata, isRowDeletion ? Columns.NONE : Columns.from(cells.keySet()));

        //if (isRowDeletion)
        //{
        //    update.deletionInfo().add(new RangeTombstone(clustering.withEOC(ClusteringPrefix.EOC.START),
        //                                                 clustering.withEOC(ClusteringPrefix.EOC.END),
        //                                                 timestamp, ldt), metadata.comparator);
        //    return mutation;
        //}

        //Rows.Writer writer = update.writer();
        //writer.setClustering(clustering);

        //// If a CQL3 table, add the row marker
        //if (metadata.isCQL3Table())
        //    writer.setTimestamp(timestamp);

        //DeletionTime dt = new SimpleDeletionTime(timestamp - 1, ldt);
        //for (ColumnDefinition c : collectionsToReset)
        //    writer.setComplexDeletion(c, dt);

        //for (Map.Entry<ColumnDefinition, Set<Cell>> entry : cells.entrySet())
        //    for (Cell cell : entry.getValue())
        //        writer.addCell(cell);

        //writer.endOfRow();
        //return mutation;
    }
}
