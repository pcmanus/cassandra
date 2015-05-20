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
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

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

    private Row.Writer regularWriter;
    private Row.Writer staticWriter;

    private boolean hasSetClustering;
    private boolean useRowMarker = true;

    private RowUpdateBuilder(PartitionUpdate update, long timestamp, int ttl, Mutation mutation)
    {
        this.update = update;

        this.defaultLiveness = SimpleLivenessInfo.forUpdate(timestamp, ttl, update.nowInSec(), update.metadata());
        this.deletionLiveness = SimpleLivenessInfo.forDeletion(timestamp, update.nowInSec());
        this.deletionTime = new SimpleDeletionTime(timestamp, update.nowInSec());

        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        this.mutation = mutation == null ? new Mutation(update.metadata().ksName, update.partitionKey()).add(update) : mutation;
    }

    private Row.Writer writer()
    {
        assert staticWriter == null : "Cannot update both static and non-static columns with the same RowUpdateBuilder object";
        if (regularWriter == null)
        {
            regularWriter = update.writer();

            // If a CQL table, add the "row marker"
            if (update.metadata().isCQLTable() && useRowMarker)
                regularWriter.writePartitionKeyLivenessInfo(defaultLiveness);
        }
        return regularWriter;
    }

    private Row.Writer staticWriter()
    {
        assert regularWriter == null : "Cannot update both static and non-static columns with the same RowUpdateBuilder object";
        if (staticWriter == null)
            staticWriter = update.staticWriter();
        return staticWriter;
    }

    private Row.Writer writer(ColumnDefinition c)
    {
        return c.isStatic() ? staticWriter() : writer();
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Object partitionKey)
    {
        this(metadata, FBUtilities.nowInSeconds(), timestamp, partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, int localDeletionTime, long timestamp, Object partitionKey)
    {
        this(metadata, localDeletionTime, timestamp, metadata.getDefaultTimeToLive(), partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Object partitionKey)
    {
        this(metadata, FBUtilities.nowInSeconds(), timestamp, ttl, partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, int localDeletionTime, long timestamp, int ttl, Object partitionKey)
    {
        this(new PartitionUpdate(metadata, makeKey(metadata, partitionKey), metadata.partitionColumns(), 1, localDeletionTime), timestamp, ttl, null);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Mutation mutation)
    {
        this(metadata, timestamp, LivenessInfo.NO_TTL, mutation);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Mutation mutation)
    {
        this(getOrAdd(metadata, mutation, FBUtilities.nowInSeconds()), timestamp, ttl, mutation);
    }

    // This must be called before any addition or deletion if used.
    public RowUpdateBuilder noRowMarker()
    {
        this.useRowMarker = false;
        return this;
    }

    public RowUpdateBuilder clustering(Object... clusteringValues)
    {
        assert clusteringValues.length == update.metadata().comparator.size()
            : "Invalid clustering values length. Expected: " + update.metadata().comparator.size() + " got: " + clusteringValues.length;
        hasSetClustering = true;
        if (clusteringValues.length > 0)
            Rows.writeClustering(update.metadata().comparator.make(clusteringValues), writer());
        return this;
    }

    public Mutation build()
    {
        Row.Writer writer = regularWriter == null ? staticWriter : regularWriter;
        if (writer != null)
            writer.endOfRow();
        return mutation;
    }

    public PartitionUpdate buildUpdate()
    {
        build();
        return update;
    }

    private static void deleteRow(PartitionUpdate update, long timestamp, Object...clusteringValues)
    {
        assert clusteringValues.length == update.metadata().comparator.size() : "Cannot delete a static row with this method";

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
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

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
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        assert c.isStatic() || update.metadata().comparator.size() == 0 || hasSetClustering : "Cannot set non static column " + c + " since no clustering hasn't been provided";
        assert c.type.isCollection() && c.type.isMultiCell();
        writer(c).writeComplexDeletion(c, new SimpleDeletionTime(defaultLiveness.timestamp() - 1, update.nowInSec()));
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(RangeTombstone rt)
    {
        update.addRangeTombstone(rt);
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(Slice slice)
    {
        update.addRangeTombstone(slice, deletionTime);
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(Object start, Object end)
    {
        ClusteringComparator cmp = update.metadata().comparator;
        Slice slice = Slice.make(cmp, cmp.make(start), cmp.make(end));
        return addRangeTombstone(slice);
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        return add(c, value);
    }

    public RowUpdateBuilder add(ColumnDefinition columnDefinition, Object value)
    {
        assert columnDefinition.isStatic() || update.metadata().comparator.size() == 0 || hasSetClustering : "Cannot set non static column " + columnDefinition + " since no clustering hasn't been provided";
        if (value == null)
            writer(columnDefinition).writeCell(columnDefinition, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, deletionLiveness, null);
        else
            writer(columnDefinition).writeCell(columnDefinition, false, bb(value, columnDefinition.type), defaultLiveness, null);
        return this;
    }

    public RowUpdateBuilder delete(String columnName)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        return delete(c);
    }

    public RowUpdateBuilder delete(ColumnDefinition columnDefinition)
    {
        return add(columnDefinition, null);
    }

    private ByteBuffer bb(Object value, AbstractType<?> type)
    {
        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        if (type.isCounter())
        {
            // See UpdateParameters.addCounter()
            assert value instanceof Long : "Attempted to adjust Counter cell with non-long value.";
            return CounterContext.instance().createGlobal(CounterId.getLocalId(), 1, (Long)value);
        }
        return ((AbstractType)type).decompose(value);
    }

    public RowUpdateBuilder addMapEntry(String columnName, Object key, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || update.metadata().comparator.size() == 0 || hasSetClustering : "Cannot set non static column " + c + " since no clustering hasn't been provided";
        assert c.type instanceof MapType;
        MapType mt = (MapType)c.type;
        writer(c).writeCell(c, false, bb(value, mt.getValuesType()), defaultLiveness, CellPath.create(bb(key, mt.getKeysType())));
        return this;
    }

    public RowUpdateBuilder addListEntry(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || hasSetClustering : "Cannot set non static column " + c + " since no clustering hasn't been provided";
        assert c.type instanceof ListType;
        ListType lt = (ListType)c.type;
        writer(c).writeCell(c, false, bb(value, lt.getElementsType()), defaultLiveness, CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())));
        return this;
    }

    private ColumnDefinition getDefinition(String name)
    {
        return update.metadata().getColumnDefinition(new ColumnIdentifier(name, true));
    }

    public AtomIterator atomIterator()
    {
        return update.atomIterator();
    }
}
