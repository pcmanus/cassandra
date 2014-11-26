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
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Convenience object to create updates.
 *
 * This is meant for system table update, when performance is not of the utmost importance.
 */
public class RowUpdateBuilder
{
    private final PartitionUpdate update;
    private final long timestamp;
    private final int ldt;
    private final Mutation mutation;

    private final Row.Writer writer;

    private final SortedMap<ColumnDefinition, Set<Cell>> cells = new TreeMap<>();
    private final Set<ColumnDefinition> collectionsToReset = new HashSet<>();

    private RowUpdateBuilder(PartitionUpdate update, long timestamp, Mutation mutation, Object[] clusteringValues)
    {
        this.update = update;
        this.timestamp = timestamp;
        this.ldt = FBUtilities.nowInSeconds();
        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        this.mutation = mutation == null ? new Mutation(update.metadata().ksName, update.partitionKey()).add(update) : mutation;
        this.writer = update.writer(false);

        // If a CQL3 table, add the row marker
        if (update.metadata().isCQL3Table())
            writer.writeTimestamp(timestamp);

        writer.writeClustering(update.metadata().comparator.make(clusteringValues));
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Object partitionKey, Object... clusteringValues)
    {
        this(new PartitionUpdate(metadata, makeKey(metadata, partitionKey), metadata.partitionColumns(), 1), timestamp, null, clusteringValues);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Mutation mutation, Object... clusteringValues)
    {
        this(getOrAdd(metadata, mutation), timestamp, mutation, clusteringValues);
    }

    public Mutation build()
    {
        writer.endOfRow();
        return mutation;
    }

    private static void deleteRow(PartitionUpdate update, long timestamp, Object...clusteringValues)
    {
        ClusteringPrefix clustering = update.metadata().comparator.make(clusteringValues);
        RangeTombstone rt = new RangeTombstone(clustering.withEOC(ClusteringPrefix.EOC.START),
                                               clustering.withEOC(ClusteringPrefix.EOC.END),
                                               timestamp,
                                               FBUtilities.nowInSeconds());
        update.addRangeTombstone(rt);
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Mutation mutation, Object... clusteringValues)
    {
        deleteRow(getOrAdd(metadata, mutation), timestamp, clusteringValues);
        return mutation;
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Object key, Object... clusteringValues)
    {
        PartitionUpdate update = new PartitionUpdate(metadata, makeKey(metadata, key), metadata.partitionColumns(), 0);
        deleteRow(update, timestamp, clusteringValues);
        return new Mutation(update);
    }

    private static DecoratedKey makeKey(CFMetaData metadata, Object... partitionKey)
    {
        ByteBuffer key = SelectStatement.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return StorageService.getPartitioner().decorateKey(key);
    }

    private static PartitionUpdate getOrAdd(CFMetaData metadata, Mutation mutation)
    {
        PartitionUpdate upd = mutation.get(metadata);
        if (upd == null)
        {
            upd = new PartitionUpdate(metadata, mutation.key(), metadata.partitionColumns(), 1);
            mutation.add(upd);
        }
        return upd;
    }

    public RowUpdateBuilder resetCollection(String columnName)
    {
        writer.writeComplexDeletion(getDefinition(columnName), new SimpleDeletionTime(timestamp - 1, ldt));
        return this;
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        if (value == null)
            Cells.writeTombstone(c, timestamp, writer);
        else
            Cells.writeCell(c, bb(value, c.type), timestamp, 0, update.metadata(), writer);
        return this;
    }

    private ByteBuffer bb(Object value, AbstractType<?> type)
    {
        return (value instanceof ByteBuffer) ? (ByteBuffer)value : ((AbstractType)type).decompose(value);
    }

    public RowUpdateBuilder addMapEntry(String columnName, Object key, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.type instanceof MapType;
        MapType mt = (MapType)c.type;
        Cells.writeCell(c, new CollectionPath(bb(key, mt.keys)), bb(value, mt.values), timestamp, 0, update.metadata(), writer);
        return this;
    }

    public RowUpdateBuilder addListEntry(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.type instanceof ListType;
        ListType lt = (ListType)c.type;
        Cells.writeCell(c, new CollectionPath(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), bb(value, lt.elements), timestamp, 0, update.metadata(), writer);
        return this;
    }

    private ColumnDefinition getDefinition(String name)
    {
        return update.metadata().getColumnDefinition(new ColumnIdentifier(name, false));
    }
}
