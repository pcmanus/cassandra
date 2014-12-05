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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 */
public abstract class ReadCommand
{
    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    public static final IVersionedSerializer<ReadCommand> legacyRangeSliceCommandSerializer = new LegacyRangeSliceCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyPagedRangeCommandSerializer = new LegacyPagedRangeCommandSerializer();

    private final CFMetaData metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final DataLimits limits;

    private boolean isDigestQuery;

    protected ReadCommand(CFMetaData metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          DataLimits limits)
    {
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.limits = limits;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public int nowInSec()
    {
        return nowInSec;
    }

    public abstract long getTimeout();

    // Filters on CQL columns (will be handled either by a 2ndary index if
    // there is one, or by on-replica filtering otherwise)
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    public DataLimits limits()
    {
        return limits;
    }

    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    public void setDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
    }

    public abstract ReadCommand copy();

    protected abstract PartitionIterator queryStorage(ColumnFamilyStore cfs);

    public ReadResponse makeResponse(PartitionIterator iter)
    {
        return ReadResponse.create(iter, isDigestQuery());
    }

    public PartitionIterator executeLocally(ColumnFamilyStore cfs)
    {
        SecondaryIndexSearcher searcher = cfs.indexManager.getBestIndexSearcherFor(this);
        PartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs)
                                         : searcher.search(this);

        // The idxSearcher.search call above will do this (but is abstracted for custom 2ndary index)
        //   PartitionIterator indexIterator = queryIndex();
        //   return filter(getDataIterator(indexIterator));

        // TODO: we should push the dropping of columns down the layers because
        // 1) it'll be more efficient
        // 2) it could help us solve #6276
        // But there is not reason not to do this as a followup so keeping it here for now (we'll have
        // to be wary of cached row if we move this down the layers)
        if (!metadata().getDroppedColumns().isEmpty())
            resultIterator = PartitionIterators.removeDroppedColumns(resultIterator, metadata().getDroppedColumns());

        // TODO: We need to populate the following metrics at the end of the count. Could be part of the
        // query counter filtering (in close)
        //       metric.tombstoneScannedHistogram.update(((SliceQueryFilter) filter.filter).lastIgnored());
        //       metric.liveScannedHistogram.update(((SliceQueryFilter) filter.filter).lastLive());

        // TODO: We'll currently do filtering by the columnFilter here because it's convenient. However,
        // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
        // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
        // processing we do on it).
        return limits().filter(columnFilter().filter(resultIterator), nowInSec());
    }

    public MessageOut<ReadCommand> createMessage()
    {
        // TODO: we should use different verbs for old message (RANGE_SLICE, PAGED_RANGE)
        return new MessageOut<>(MessagingService.Verb.READ, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public long serializedSize(ReadCommand command, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
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
