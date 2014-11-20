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
package org.apache.cassandra.db.partitions;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Stores updates on a partition.
 */
public class PartitionUpdate extends AbstractPartitionData implements Iterable<Row>
{
    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private boolean isSorted;

    private final Rows.Writer writer = new Writer();

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            DeletionInfo delInfo,
                            RowDataBlock data,
                            PartitionColumns columns,
                            int initialRowCapacity)
    {
        super(metadata, key, delInfo, columns, data, initialRowCapacity);
    }

    public PartitionUpdate(CFMetaData metadata,
                           DecoratedKey key,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata, key, DeletionInfo.live(), new RowDataBlock(columns.regulars, initialRowCapacity), columns, initialRowCapacity);
    }

    public static PartitionUpdate fromBytes(ByteBuffer bytes)
    {
        // This is for paxos and so we need to be able to read the previous ColumnFamily format.
        // The simplest solution is probably to include a version in the paxos table and assume
        // that no version == old serialization format
        // TODO
        throw new UnsupportedOperationException();
    }

    public static ByteBuffer toBytes(PartitionUpdate update)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static PartitionUpdate fullPartitionDelete(CFMetaData metadata, DecoratedKey key, long timestamp)
    {
        return new PartitionUpdate(metadata,
                                   key,
                                   new DeletionInfo(timestamp, FBUtilities.nowInSeconds()),
                                   new RowDataBlock(Columns.NONE, 0),
                                   PartitionColumns.NONE,
                                   0);
    }

    public int operationCount()
    {
        return rowCount()
             + deletionInfo().rangeCount()
             + (deletionInfo().getTopLevelDeletion().isLive() ? 0 : 1);
    }

    public int dataSize()
    {
        // Used to reject batches that are too big (see BatchStatement.verifyBatchSize)
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Row> iterator()
    {
        maybeSort();
        return super.iterator();
    }

    @Override
    public SearchIterator<ClusteringPrefix, Row> searchIterator()
    {
        maybeSort();
        return super.searchIterator();
    }

    public PartitionUpdate addAll(PartitionUpdate update)
    {
        // TODO (but do we need IMutation.addAll(), which is the only place that needs it)
        throw new UnsupportedOperationException();
        //assert !isSorted;

        //deletionInfo.add(update.deletionInfo);
        //rowUpdates.addAll(update.rowUpdates);
        //return this;
    }

    public void validate()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public long maxTimestamp()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public List<CounterMark> collectCounterMarks()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    private void maybeSort()
    {
        if (isSorted)
            return;

        sort();
    }

    private synchronized void sort()
    {
        // TODO
        throw new UnsupportedOperationException();
        //if (isSorted || rowUpdates.isEmpty())
        //    return;

        //// Check for duplicate to reconcile. Note that we reconcile in place.
        //ClusteringComparator comparator = metadata.comparator;

        //// Sort the array - will still potentially contain duplicate (non-reconciled) rows
        //Collections.sort(rowUpdates, comparator);

        //int previous = 0; // The last element that was set
        //for (int current = 1; current < rowUpdates.size(); current++)
        //{
        //    // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
        //    RowUpdate prev = rowUpdates.get(previous);
        //    RowUpdate curr = rowUpdates.get(current);
        //    if (comparator.compare(prev, curr) == 0)
        //    {
        //        rowUpdates.set(previous, prev.mergeTo(curr, SecondaryIndexManager.nullUpdater));
        //    }
        //    else
        //    {
        //        // current != previous, so simply move current just after previous if needs be
        //        ++previous;
        //        if (previous != current)
        //            rowUpdates.set(previous, curr);
        //    }
        //}

        //// previous is on the last value to keep
        //for (int i = rowUpdates.size() - 1; i > previous; i++)
        //    rowUpdates.remove(i);

        //isSorted = true;
        //hasStatic = rowUpdates.get(0).clustering() == EmptyClusteringPrefix.STATIC_PREFIX;
    }

    public Rows.Writer writer(boolean isStatic)
    {
        if (isStatic)
            throw new UnsupportedOperationException();

        return writer;
    }

    public static class PartitionUpdateSerializer implements IVersionedSerializer<PartitionUpdate>
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
            //if (version < MessagingService.VERSION_30)
            //{
            //    // TODO
            //    throw new UnsupportedOperationException();

            //    // if (cf == null)
            //    // {
            //    //     out.writeBoolean(false);
            //    //     return;
            //    // }

            //    // out.writeBoolean(true);
            //    // serializeCfId(cf.id(), out, version);
            //    // cf.getComparator().deletionInfoSerializer().serialize(cf.deletionInfo(), out, version);
            //    // ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
            //    // int count = cf.getColumnCount();
            //    // out.writeInt(count);
            //    // int written = 0;
            //    // for (Cell cell : cf)
            //    // {
            //    //     columnSerializer.serialize(cell, out);
            //    //     written++;
            //    // }
            //    // assert count == written: "Table had " + count + " columns, but " + written + " written";
            //}

            //CFMetaData metadata = update.metadata;

            //serializeCfId(metadata.cfId, out, version);

            //// TODO: we could consider writing the token (provided this is done by the partitioner,
            //// LocalPartition and BytesPartitioner wouldn't have to write anything more, and random
            //// partition would be a single long)
            //ByteBufferUtil.writeWithShortLength(update.partitionKey().getKey(), out);

            //metadata.layout().deletionInfoSerializer().serialize(update.deletionInfo(), out, version);
            //out.writeInt(update.rowUpdates.size());
            //for (RowUpdate row : update.rowUpdates)
            //    metadata.layout().rowsSerializer().serialize(row, out);
        }

        public PartitionUpdate deserialize(DataInput in, int version, LegacyLayout.Flag flag, DecoratedKey key) throws IOException
        {
            throw new UnsupportedOperationException();
            //CFMetaData metadata;
            //DeletionInfo delInfo;
            //List<RowUpdate> updates;
            //if (version < MessagingService.VERSION_30)
            //{
            //    // TODO
            //    throw new UnsupportedOperationException();
            //    //if (!in.readBoolean())
            //    //    return null;

            //    //ColumnFamily cf = factory.create(Schema.instance.getCFMetaData(deserializeCfId(in, version)));

            //    //if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
            //    //{
            //    //    SuperColumns.deserializerSuperColumnFamily(in, cf, flag, version);
            //    //}
            //    //else
            //    //{
            //    //    cf.delete(cf.getComparator().deletionInfoSerializer().deserialize(in, version));

            //    //    ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
            //    //    int size = in.readInt();
            //    //    for (int i = 0; i < size; ++i)
            //    //        cf.addColumn(columnSerializer.deserialize(in, flag));
            //    //}
            //    //return cf;
            //}
            //else
            //{
            //    metadata = Schema.instance.getCFMetaData(deserializeCfId(in, version));
            //    key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
            //    delInfo = metadata.layout().deletionInfoSerializer().deserialize(in, version);
            //    int size = in.readInt();
            //    updates = new ArrayList<>(size);
            //    throw new UnsupportedOperationException();
            //    //for (int i = 0; i < size; i++)
            //    //{
            //    //    metadata.layout().rowsSerializer().deserialize(in, version, flag, writer, metadata);
            //    //}
            //}
        }

        public PartitionUpdate deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            return serializedSize(update, TypeSizes.NATIVE, version);
        }

        public long serializedSize(PartitionUpdate update, TypeSizes sizes, int version)
        {
            throw new UnsupportedOperationException();
            //if (version < MessagingService.VERSION_30)
            //{
            //    // TODO
            //    throw new UnsupportedOperationException();
            //    //if (cf == null)
            //    //{
            //    //    return typeSizes.sizeof(false);
            //    //}
            //    //else
            //    //{
            //    //    return typeSizes.sizeof(true)  /* nullness bool */
            //    //        + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
            //    //        + contentSerializedSize(cf, typeSizes, version);
            //    //}
            //}

            //int size = cfIdSerializedSize(update.metadata().cfId,  sizes, version);
            //size += ByteBufferUtil.serializedSizeWithShortLength(update.partitionKey().getKey(), sizes);
            //size += update.metadata().layout().deletionInfoSerializer().serializedSize(update.deletionInfo(), version);

            //size += sizes.sizeof(update.rowUpdates.size());
            //for (RowUpdate row : update.rowUpdates)
            //    size += update.metadata().layout().rowsSerializer().serializedSize(row, sizes);
            //return size;
        }

        public void serializeCfId(UUID cfId, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(cfId, out, version);
        }

        public UUID deserializeCfId(DataInput in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            if (Schema.instance.getCF(cfId) == null)
                throw new UnknownColumnFamilyException("Couldn't find cfId=" + cfId, cfId);

            return cfId;
        }

        public int cfIdSerializedSize(UUID cfId, TypeSizes typeSizes, int version)
        {
            return typeSizes.sizeof(cfId);
        }
    }

    /**
     * This is basically a pointer inside this PartitionUpdate that allows to set counter updates based on
     * the existing value read during the read-before-write.
     */
    public static class CounterMark
    {
        public ClusteringPrefix clustering()
        {
            throw new UnsupportedOperationException();
        }

        public ColumnDefinition column()
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer value()
        {
            throw new UnsupportedOperationException();
        }

        public void setValue(ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }
    }
}
