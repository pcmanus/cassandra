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
import org.apache.cassandra.utils.Sorting;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Stores updates on a partition.
 */
public class PartitionUpdate extends AbstractPartitionData implements Iterable<Row>, Sorting.Sortable
{
    private boolean isSorted;

    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private final Writer writer = new Writer();

    // Used by compare for the sake of implementing the Sorting.Sortable interface
    private final ReusableClusteringPrefix p1 = new ReusableClusteringPrefix();
    private final ReusableClusteringPrefix p2 = new ReusableClusteringPrefix();

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
             + deletionInfo.rangeCount()
             + (deletionInfo.getTopLevelDeletion().isLive() ? 0 : 1);
    }

    // Note that one shouldn't use the return deletionInfo to add range tombstone to
    // this object as this wouldn't update the stats properly (use addRangeTombstone instead)
    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
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
    protected SeekableAtomIterator seekableAtomIterator(PartitionColumns columns, boolean reversed)
    {
        maybeSort();
        return super.seekableAtomIterator(columns, reversed);
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = metadata();
        sb.append(String.format("Update[%s.%s] key=%s columns=%s\n",
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(partitionKey().getKey()),
                    columns()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("-----\n").append(Rows.toString(metadata, staticRow()));

        Iterator<Row> iterator = iterator();
        while (iterator.hasNext())
            sb.append("-----\n").append(Rows.toString(metadata, iterator.next(), true));

        sb.append("-----\n");
        return sb.toString();
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
        if (isSorted)
            return;

        if (rows <= 1)
        {
            isSorted = true;
            return;
        }

        // Sort the rows - will still potentially contain duplicate (non-reconciled) rows
        Sorting.sort(this);

        int nowInSec = FBUtilities.nowInSeconds();
        // Now find duplicates and merge them together
        int previous = 0; // The last element that was set
        for (int current = 1; current < rows; current++)
        {
            // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
            int cmp = compare(previous, current);
            if (cmp == 0)
            {
                // current and previous are the same row. Merge current into previous
                // (and so previous + 1 will be "free").
                data.merge(current, previous, nowInSec);
            }
            else
            {
                // data[current] != [previous], so move current just after previous if needs be
                ++previous;
                if (previous != current)
                    data.move(current, previous);
            }
        }

        // previous is on the last value to keep
        rows = previous + 1;

        isSorted = true;
    }

    public Row.Writer writer(boolean isStatic)
    {
        if (isStatic)
            throw new UnsupportedOperationException();

        return writer;
    }

    public int size()
    {
        return rows;
    }

    public int compare(int i, int j)
    {
        return metadata.comparator.compare(p1.setTo(i), p2.setTo(j));
    }

    public void swap(int i, int j)
    {
        int cs = metadata.clusteringColumns().size();
        for (int k = 0; k < cs; k++)
        {
            ByteBuffer tmp = clusterings[j * cs + k];
            clusterings[j * cs + k] = clusterings[i * cs + k];
            clusterings[i * cs + k] = tmp;
        }

        long tmp = timestamps[j];
        timestamps[j] = timestamps[i];
        timestamps[i] = tmp;

        data.swap(i, j);
    }

    private class ReusableClusteringPrefix extends AbstractClusteringPrefix
    {
        private int row;

        public ReusableClusteringPrefix setTo(int row)
        {
            this.row = row;
            return this;
        }

        public int size()
        {
            return metadata.clusteringColumns().size();
        }

        public ByteBuffer get(int i)
        {
            int base = row * metadata.clusteringColumns().size();
            return clusterings[base + i];
        }
    };

    public static class PartitionUpdateSerializer
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();

                // if (cf == null)
                // {
                //     out.writeBoolean(false);
                //     return;
                // }

                // out.writeBoolean(true);
                // serializeCfId(cf.id(), out, version);
                // cf.getComparator().deletionInfoSerializer().serialize(cf.deletionInfo(), out, version);
                // ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                // int count = cf.getColumnCount();
                // out.writeInt(count);
                // int written = 0;
                // for (Cell cell : cf)
                // {
                //     columnSerializer.serialize(cell, out);
                //     written++;
                // }
                // assert count == written: "Table had " + count + " columns, but " + written + " written";
            }

            AtomIteratorSerializer.serializer.serialize(update.seekableAtomIterator(update.columns(), false), out, version, update.rows);
        }

        public static PartitionUpdate deserialize(DataInput in, int version, LegacyLayout.Flag flag, DecoratedKey key) throws IOException
        {
            // TODO Add back the support of the 'flag'
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
                //if (!in.readBoolean())
                //    return null;

                //ColumnFamily cf = factory.create(Schema.instance.getCFMetaData(deserializeCfId(in, version)));

                //if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
                //{
                //    SuperColumns.deserializerSuperColumnFamily(in, cf, flag, version);
                //}
                //else
                //{
                //    cf.delete(cf.getComparator().deletionInfoSerializer().deserialize(in, version));

                //    ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                //    int size = in.readInt();
                //    for (int i = 0; i < size; ++i)
                //        cf.addColumn(columnSerializer.deserialize(in, flag));
                //}
                //return cf;
            }

            assert key == null;

            AtomIteratorSerializer.FullHeader fh = AtomIteratorSerializer.serializer.deserializeHeader(in, version);
            assert !fh.header.isReversed && !fh.isEmpty;
            // TODO: get a better initial capacity
            int rowCapacity = fh.rowEstimate > 0 ? fh.rowEstimate : 4;

            PartitionUpdate upd = new PartitionUpdate(fh.header.metadata, fh.header.key, fh.header.columns, rowCapacity);
            upd.addPartitionDeletion(fh.partitionDeletion);
            upd.staticRow = fh.staticRow;
            upd.isSorted = true;

            RangeTombstoneMarker.Writer markerWriter = upd.new RangeTombstoneCollector();
            AtomIteratorSerializer.serializer.deserializeAtoms(in, version, fh.header, upd.writer(false), markerWriter);
            return upd;
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
                //if (cf == null)
                //{
                //    return typeSizes.sizeof(false);
                //}
                //else
                //{
                //    return typeSizes.sizeof(true)  /* nullness bool */
                //        + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                //        + contentSerializedSize(cf, typeSizes, version);
                //}
            }

            return AtomIteratorSerializer.serializer.serializedSize(update.seekableAtomIterator(update.columns(), false), version, update.rows);
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
