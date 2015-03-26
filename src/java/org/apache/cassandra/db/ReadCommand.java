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

import java.io.DataInput;
import java.io.IOException;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand extends ReadQuery
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    public static final IVersionedSerializer<ReadCommand> legacyRangeSliceCommandSerializer = new LegacyRangeSliceCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyPagedRangeCommandSerializer = new LegacyPagedRangeCommandSerializer();

    private final Kind kind;
    private final CFMetaData metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final DataLimits limits;

    private boolean isDigestQuery;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInput in, int version, boolean isDigest, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, DataLimits limits) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private SelectionDeserializer selectionDeserializer;

        private Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          boolean isDigestQuery,
                          CFMetaData metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          DataLimits limits)
    {
        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.limits = limits;
    }

    protected abstract void serializeSelection(DataOutputPlus out, int version) throws IOException;
    protected abstract long selectionSerializedSize(int version);

    /**
     * The metadata for the table queried.
     *
     * @return the metadata for the table queried.
     */
    public CFMetaData metadata()
    {
        return metadata;
    }

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public int nowInSec()
    {
        return nowInSec;
    }

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout();

    // Filters on CQL columns (will be handled either by a 2ndary index if
    // there is one, or by on-replica filtering otherwise)
    /**
     * Filters/Resrictions on CQL columns.
     * <p>
     * This contains those restrictions that are not directly handled by the
     * {@code PartitionFilter}. More specifically, this includes any non-PK columns
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirerly by the partition filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the restrictions on CQL columns that aren't directly satisfied by the
     * underlying {@code PartitionFilter} of this command.
     */
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    /**
     * The limits set on this query.
     *
     * @return the limits set on this query.
     */
    public DataLimits limits()
    {
        return limits;
    }

    /**
     * Whether this query is a digest one or not.
     *
     * @return Whether this query is a digest query.
     */
    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    /**
     * Sets whether this command should be a digest one or not.
     *
     * @param isDigestQuery whether the command should be set as a digest one or not.
     * @return this read command.
     */
    public ReadCommand setIsDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
        return this;
    }

    /**
     * The columns queried by this command.
     *
     * @return the columns queried by this command.
     */
    public abstract PartitionColumns queriedColumns();

    /**
     * The partition filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code PartitionFilter} to use for the partition of key {@code key}.
     */
    public abstract PartitionFilter partitionFilter(DecoratedKey key);

    /**
     * Returns a copy of this command.
     *
     * @return a copy of this command.
     */
    public abstract ReadCommand copy();

    /**
     * Whether the provided row, identified by its primary key components, is selected by
     * this read command.
     *
     * @param partitionKey the partition key for the row to test.
     * @param clustering the clustering for the row to test.
     *
     * @return whether the row of partition key {@code partitionKey} and clustering
     * {@code clustering} is selected by this command.
     */
    public abstract boolean selects(DecoratedKey partitionKey, Clustering clustering);

    protected abstract PartitionIterator queryStorage(ColumnFamilyStore cfs);

    public ReadResponse makeResponse(PartitionIterator iter, boolean isLocalDataQuery)
    {
        if (isDigestQuery())
            return ReadResponse.createDigestResponse(iter);
        else if (isLocalDataQuery)
            return ReadResponse.createLocalDataResponse(iter);
        else
            return ReadResponse.createDataResponse(iter);
    }

    /**
     * Executes this command on the local host.
     *
     * @param cfs the store for the table queried by this command.
     *
     * @return an iterator over the result of executing this command locally.
     */
    public PartitionIterator executeLocally(ColumnFamilyStore cfs)
    {
        SecondaryIndexSearcher searcher = cfs.indexManager.getBestIndexSearcherFor(this);
        PartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs)
                                         : searcher.search(this);

        try
        {
            resultIterator = withMetricsRecording(resultIterator, cfs.metric);

            // TODO: we should push the dropping of columns down the layers because
            // 1) it'll be more efficient
            // 2) it could help us solve #6276
            // But there is not reason not to do this as a followup so keeping it here for now (we'll have
            // to be wary of cached row if we move this down the layers)
            if (!metadata().getDroppedColumns().isEmpty())
                resultIterator = PartitionIterators.removeDroppedColumns(resultIterator, metadata().getDroppedColumns());

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            ColumnFilter updatedFilter = searcher == null
                                       ? columnFilter()
                                       : columnFilter().without(searcher.primaryClause(this));

            // TODO: We'll currently do filtering by the columnFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return limits().filter(updatedFilter.filter(resultIterator));
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    public DataIterator executeLocally()
    {
        return PartitionIterators.asDataIterator(executeLocally(Keyspace.openAndGetStore(metadata())));
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private PartitionIterator withMetricsRecording(PartitionIterator iter, final ColumnFamilyMetrics metric)
    {
        return new WrappingPartitionIterator(iter)
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public AtomIterator computeNext(AtomIterator iter)
            {
                currentKey = iter.partitionKey();

                return new WrappingAtomIterator(iter)
                {
                    public Atom next()
                    {
                        Atom atom = super.next();
                        if (atom.kind() == Atom.Kind.ROW)
                        {
                            Row row = (Row)atom;
                            if (row.hasLiveData())
                                ++liveRows;
                            for (Cell cell : row)
                                if (!cell.isLive(ReadCommand.this.nowInSec()))
                                    countTombstone(row.clustering());
                        }
                        else
                        {
                            countTombstone(atom.clustering());
                        }

                        return atom;
                    }

                    private void countTombstone(ClusteringPrefix clustering)
                    {
                        ++tombstones;
                        if (tombstones > failureThreshold)
                        {
                            String query = ReadCommand.this.toCQLString();
                            Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                            throw new TombstoneOverwhelmingException(tombstones, query, ReadCommand.this.metadata(), currentKey, clustering);
                        }
                    }
                };
            }

            @Override
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    metric.tombstoneScannedHistogram.update(tombstones);
                    metric.liveScannedHistogram.update(liveRows);

                    boolean warnTombstones = tombstones > warningThreshold;
                    if (warnTombstones)
                        logger.warn("Read {} live rows and {} tombstoned cells for query {} (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toString());

                    Tracing.trace("Read {} live and {} tombstoned cells{}", new Object[]{ liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : "") });
                }
            }
        };
    }

    /**
     * Creates a message for this command.
     */
    public MessageOut<ReadCommand> createMessage()
    {
        // TODO: we should use different verbs for old message (RANGE_SLICE, PAGED_RANGE)
        return new MessageOut<>(MessagingService.Verb.READ, this, serializer);
    }

    protected abstract boolean appendCQLWhereClause(StringBuilder sb);

    /**
     * Recreate the CQL string corresponding to this query.
     * <p>
     * Note that in general the returned string will not be exactly the original user string, first
     * because there isn't always a single syntax for a given query,  but also because we don't have
     * all the information needed (we know the non-PK columns queried but not the PK ones as internally
     * we query them all). So this shouldn't be relied too strongly, but this should be good enough for
     * debugging purpose which is what this is for.
     */
    public String toCQLString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (queriedColumns().equals(metadata().partitionColumns()))
        {
            sb.append("*");
        }
        else
        {
            sb.append(ColumnDefinition.toCQLString(Iterables.concat(metadata().partitionKeyColumns(), metadata().clusteringColumns())));
            if (!queriedColumns().isEmpty())
                sb.append(", ").append(ColumnDefinition.toCQLString(queriedColumns().selectOrderIterator()));
        }

        sb.append(" FROM ").append(metadata().ksName).append(".").append(metadata.cfName);
        boolean needAnd = appendCQLWhereClause(sb);
        String cond = columnFilter.toString();
        if (!cond.isEmpty())
            sb.append(needAnd ? " AND " : "").append(cond);

        if (limits() != DataLimits.NONE)
            sb.append(" ").append(limits());
        return sb.toString();
    }

    private static class Serializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            out.writeByte((command.kind.ordinal() | (command.isDigestQuery ? 0x80 : 0)));
            CFMetaData.serializer.serialize(command.metadata(), out, version);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            int kindAndDigest = in.readByte();
            Kind kind = Kind.values()[kindAndDigest & 0x7F];
            boolean isDigest = (kindAndDigest & 0x80) != 0;
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);
            DataLimits limits = DataLimits.serializer.deserialize(in, version);

            return kind.selectionDeserializer.deserialize(in, version, isDigest, metadata, nowInSec, columnFilter, limits);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            TypeSizes sizes = TypeSizes.NATIVE;

            return 1 // kind + isDigestQuery
                 + CFMetaData.serializer.serializedSize(command.metadata(), version, sizes)
                 + sizes.sizeof(command.nowInSec())
                 + ColumnFilter.serializer.serializedSize(command.columnFilter(), version)
                 + DataLimits.serializer.serializedSize(command.limits(), version)
                 + command.selectionSerializedSize(version);
        }
    }

    /*
     * Deserialize pre-3.0 RangeSliceCommand for backward compatibility sake
     */
    private static class LegacyRangeSliceCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //         out.writeUTF(sliceCommand.keyspace);
            //         out.writeUTF(sliceCommand.columnFamily);
            //         out.writeLong(sliceCommand.timestamp);
            // 
            //         CFMetaData metadata = Schema.instance.getCFMetaData(sliceCommand.keyspace, sliceCommand.columnFamily);
            // 
            //         metadata.comparator.diskAtomFilterSerializer().serialize(sliceCommand.predicate, out, version);
            // 
            //         if (sliceCommand.rowFilter == null)
            //         {
            //             out.writeInt(0);
            //         }
            //         else
            //         {
            //             out.writeInt(sliceCommand.rowFilter.size());
            //             for (IndexExpression expr : sliceCommand.rowFilter)
            //             {
            //                 ByteBufferUtil.writeWithShortLength(expr.column, out);
            //                 out.writeInt(expr.operator.ordinal());
            //                 ByteBufferUtil.writeWithShortLength(expr.value, out);
            //             }
            //         }
            //         AbstractBounds.serializer.serialize(sliceCommand.keyRange, out, version);
            //         out.writeInt(sliceCommand.maxResults);
            //         out.writeBoolean(sliceCommand.countCQL3Rows);
            //         out.writeBoolean(sliceCommand.isPaging);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //         String keyspace = in.readUTF();
            //         String columnFamily = in.readUTF();
            //         long timestamp = in.readLong();
            // 
            //         CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            // 
            //         IDiskAtomFilter predicate = metadata.comparator.diskAtomFilterSerializer().deserialize(in, version);
            // 
            //         List<IndexExpression> rowFilter;
            //         int filterCount = in.readInt();
            //         rowFilter = new ArrayList<>(filterCount);
            //         for (int i = 0; i < filterCount; i++)
            //         {
            //             IndexExpression expr;
            //             expr = new IndexExpression(ByteBufferUtil.readWithShortLength(in),
            //                                        IndexExpression.Operator.findByOrdinal(in.readInt()),
            //                                        ByteBufferUtil.readWithShortLength(in));
            //             rowFilter.add(expr);
            //         }
            //         AbstractBounds<RowPosition> range = AbstractBounds.serializer.deserialize(in, version).toRowBounds();
            // 
            //         int maxResults = in.readInt();
            //         boolean countCQL3Rows = in.readBoolean();
            //         boolean isPaging = in.readBoolean();
            //         return new RangeSliceCommand(keyspace, columnFamily, timestamp, predicate, range, rowFilter, maxResults, countCQL3Rows, isPaging);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
            //         long size = TypeSizes.NATIVE.sizeof(rsc.keyspace);
            //         size += TypeSizes.NATIVE.sizeof(rsc.columnFamily);
            //         size += TypeSizes.NATIVE.sizeof(rsc.timestamp);
            // 
            //         CFMetaData metadata = Schema.instance.getCFMetaData(rsc.keyspace, rsc.columnFamily);
            // 
            //         IDiskAtomFilter filter = rsc.predicate;
            // 
            //         size += metadata.comparator.diskAtomFilterSerializer().serializedSize(filter, version);
            // 
            //         if (rsc.rowFilter == null)
            //         {
            //             size += TypeSizes.NATIVE.sizeof(0);
            //         }
            //         else
            //         {
            //             size += TypeSizes.NATIVE.sizeof(rsc.rowFilter.size());
            //             for (IndexExpression expr : rsc.rowFilter)
            //             {
            //                 size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
            //                 size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
            //                 size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            //             }
            //         }
            //         size += AbstractBounds.serializer.serializedSize(rsc.keyRange, version);
            //         size += TypeSizes.NATIVE.sizeof(rsc.maxResults);
            //         size += TypeSizes.NATIVE.sizeof(rsc.countCQL3Rows);
            //         size += TypeSizes.NATIVE.sizeof(rsc.isPaging);
            //         return size;
        }
    }

    /*
     * Deserialize pre-3.0 PagedRangeCommand for backward compatibility sake
     */
    private static class LegacyPagedRangeCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {

            // TODO
            throw new UnsupportedOperationException();
            //        out.writeUTF(cmd.keyspace);
            //        out.writeUTF(cmd.columnFamily);
            //        out.writeLong(cmd.timestamp);

            //        AbstractBounds.serializer.serialize(cmd.keyRange, out, version);

            //        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            //        // SliceQueryFilter (the count is not used)
            //        SliceQueryFilter filter = (SliceQueryFilter)cmd.predicate;
            //        metadata.comparator.sliceQueryFilterSerializer().serialize(filter, out, version);

            //        // The start and stop of the page
            //        metadata.comparator.serializer().serialize(cmd.start, out);
            //        metadata.comparator.serializer().serialize(cmd.stop, out);

            //        out.writeInt(cmd.rowFilter.size());
            //        for (IndexExpression expr : cmd.rowFilter)
            //        {
            //            ByteBufferUtil.writeWithShortLength(expr.column, out);
            //            out.writeInt(expr.operator.ordinal());
            //            ByteBufferUtil.writeWithShortLength(expr.value, out);
            //        }

            //        out.writeInt(cmd.limit);
            //        if (version >= MessagingService.VERSION_21)
            //            out.writeBoolean(cmd.countCQL3Rows);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        String keyspace = in.readUTF();
            //        String columnFamily = in.readUTF();
            //        long timestamp = in.readLong();

            //        AbstractBounds<RowPosition> keyRange = AbstractBounds.serializer.deserialize(in, version).toRowBounds();

            //        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);

            //        SliceQueryFilter predicate = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);

            //        Composite start = metadata.comparator.serializer().deserialize(in);
            //        Composite stop =  metadata.comparator.serializer().deserialize(in);

            //        int filterCount = in.readInt();
            //        List<IndexExpression> rowFilter = new ArrayList<IndexExpression>(filterCount);
            //        for (int i = 0; i < filterCount; i++)
            //        {
            //            IndexExpression expr = new IndexExpression(ByteBufferUtil.readWithShortLength(in),
            //                                                       IndexExpression.Operator.findByOrdinal(in.readInt()),
            //                                                       ByteBufferUtil.readWithShortLength(in));
            //            rowFilter.add(expr);
            //        }

            //        int limit = in.readInt();
            //        boolean countCQL3Rows = version >= MessagingService.VERSION_21
            //                              ? in.readBoolean()
            //                              : predicate.compositesToGroup >= 0 || predicate.count != 1; // See #6857
            //        return new PagedRangeCommand(keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit, countCQL3Rows);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            throw new UnsupportedOperationException();
            //        long size = 0;

            //        size += TypeSizes.NATIVE.sizeof(cmd.keyspace);
            //        size += TypeSizes.NATIVE.sizeof(cmd.columnFamily);
            //        size += TypeSizes.NATIVE.sizeof(cmd.timestamp);

            //        size += AbstractBounds.serializer.serializedSize(cmd.keyRange, version);

            //        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            //        size += metadata.comparator.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)cmd.predicate, version);

            //        size += metadata.comparator.serializer().serializedSize(cmd.start, TypeSizes.NATIVE);
            //        size += metadata.comparator.serializer().serializedSize(cmd.stop, TypeSizes.NATIVE);

            //        size += TypeSizes.NATIVE.sizeof(cmd.rowFilter.size());
            //        for (IndexExpression expr : cmd.rowFilter)
            //        {
            //            size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
            //            size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
            //            size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            //        }

            //        size += TypeSizes.NATIVE.sizeof(cmd.limit);
            //        if (version >= MessagingService.VERSION_21)
            //            size += TypeSizes.NATIVE.sizeof(cmd.countCQL3Rows);
            //        return size;
        }
    }

    // From old ReadCommand
    //class ReadCommandSerializer implements IVersionedSerializer<ReadCommand>
    //{
    //    public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
    //    {
    //        out.writeByte(command.commandType.serializedValue);
    //        switch (command.commandType)
    //        {
    //            case GET_BY_NAMES:
    //                SliceByNamesReadCommand.serializer.serialize(command, out, version);
    //                break;
    //            case GET_SLICES:
    //                SliceFromReadCommand.serializer.serialize(command, out, version);
    //                break;
    //            default:
    //                throw new AssertionError();
    //        }
    //    }

    //    public ReadCommand deserialize(DataInput in, int version) throws IOException
    //    {
    //        ReadCommand.Type msgType = ReadCommand.Type.fromSerializedValue(in.readByte());
    //        switch (msgType)
    //        {
    //            case GET_BY_NAMES:
    //                return SliceByNamesReadCommand.serializer.deserialize(in, version);
    //            case GET_SLICES:
    //                return SliceFromReadCommand.serializer.deserialize(in, version);
    //            default:
    //                throw new AssertionError();
    //        }
    //    }

    //    public long serializedSize(ReadCommand command, int version)
    //    {
    //        switch (command.commandType)
    //        {
    //            case GET_BY_NAMES:
    //                return 1 + SliceByNamesReadCommand.serializer.serializedSize(command, version);
    //            case GET_SLICES:
    //                return 1 + SliceFromReadCommand.serializer.serializedSize(command, version);
    //            default:
    //                throw new AssertionError();
    //        }
    //    }
    //}

}
