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

    private final LivenessInfo defaultLiveness;
    private final LivenessInfo deletionLiveness;
    private final DeletionTime deletionTime;

    private final Mutation mutation;

    private final Row.Writer writer;

    public RowUpdateBuilder(PartitionUpdate update, long timestamp, int ttl, Mutation mutation)
    {
        this.update = update;

        this.defaultLiveness = SimpleLivenessInfo.forUpdate(timestamp, ttl, update.nowInSec(), update.metadata());
        this.deletionLiveness = SimpleLivenessInfo.forDeletion(timestamp, update.nowInSec());
        this.deletionTime = new SimpleDeletionTime(timestamp, update.nowInSec());

        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        this.mutation = mutation == null ? new Mutation(update.metadata().ksName, update.partitionKey()).add(update) : mutation;
        this.writer = update.writer();

        // If a CQL3 table, add the row marker
        if (update.metadata().isCQL3Table())
            writer.writePartitionKeyLivenessInfo(defaultLiveness);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Object partitionKey)
    {
        this(metadata, timestamp, metadata.getDefaultTimeToLive(), partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Object partitionKey)
    {
        this(new PartitionUpdate(metadata, makeKey(metadata, partitionKey), metadata.partitionColumns(), 1, FBUtilities.nowInSeconds()), timestamp, ttl, null);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Mutation mutation)
    {
        this(metadata, timestamp, LivenessInfo.NO_TTL, mutation);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Mutation mutation)
    {
        this(getOrAdd(metadata, mutation, FBUtilities.nowInSeconds()), timestamp, ttl, mutation);
    }

    public RowUpdateBuilder clustering(Object... clusteringValues)
    {
        assert clusteringValues.length == update.metadata().comparator.size();
        if (clusteringValues.length > 0)
            Rows.writeClustering(update.metadata().comparator.make(clusteringValues), writer);
        return this;
    }

    public Mutation build()
    {
        writer.endOfRow();
        return mutation;
    }

    private static void deleteRow(PartitionUpdate update, long timestamp, Object...clusteringValues)
    {
        Row.Writer writer = update.writer();
        if (clusteringValues.length > 0)
            Rows.writeClustering(update.metadata().comparator.make(clusteringValues), writer);
        writer.writeRowDeletion(new SimpleDeletionTime(timestamp, FBUtilities.nowInSeconds()));
        writer.endOfRow();
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Mutation mutation, Object... clusteringValues)
    {
        deleteRow(getOrAdd(metadata, mutation, FBUtilities.nowInSeconds()), timestamp, clusteringValues);
        return mutation;
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Object key, Object... clusteringValues)
    {
        PartitionUpdate update = new PartitionUpdate(metadata, makeKey(metadata, key), metadata.partitionColumns(), 0, FBUtilities.nowInSeconds());
        deleteRow(update, timestamp, clusteringValues);
        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        return new Mutation(update.metadata().ksName, update.partitionKey()).add(update);
    }

    private static DecoratedKey makeKey(CFMetaData metadata, Object... partitionKey)
    {
        ByteBuffer key = CFMetaData.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return StorageService.getPartitioner().decorateKey(key);
    }

    private static PartitionUpdate getOrAdd(CFMetaData metadata, Mutation mutation, int nowInSec)
    {
        PartitionUpdate upd = mutation.get(metadata);
        if (upd == null)
        {
            upd = new PartitionUpdate(metadata, mutation.key(), metadata.partitionColumns(), 1, nowInSec);
            mutation.add(upd);
        }
        return upd;
    }

    public RowUpdateBuilder resetCollection(String columnName)
    {
        ColumnDefinition def = getDefinition(columnName);
        assert def.type.isCollection() && def.type.isMultiCell();
        writer.writeComplexDeletion(def, new SimpleDeletionTime(defaultLiveness.timestamp() - 1, update.nowInSec()));
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(Slice slice)
    {
        update.addRangeTombstone(slice, deletionTime);

        return this;
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        return add(c, value);
    }

    public RowUpdateBuilder add(ColumnDefinition columnDefinition, Object value)
    {
        if (value == null)
            writer.writeCell(columnDefinition, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, deletionLiveness, null);
        else
            writer.writeCell(columnDefinition, false, bb(value, columnDefinition.type), defaultLiveness, null);
        return this;
    }

    public RowUpdateBuilder delete(ColumnDefinition columnDefinition)
    {
        return add(columnDefinition, null);
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
        writer.writeCell(c, false, bb(value, mt.getValuesType()), defaultLiveness, CellPath.create(bb(key, mt.getKeysType())));
        return this;
    }

    public RowUpdateBuilder addListEntry(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.type instanceof ListType;
        ListType lt = (ListType)c.type;
        writer.writeCell(c, false, bb(value, lt.getElementsType()), defaultLiveness, CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())));
        return this;
    }

    private ColumnDefinition getDefinition(String name)
    {
        return update.metadata().getColumnDefinition(new ColumnIdentifier(name, true));
    }
}
