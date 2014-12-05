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
package org.apache.cassandra.db.filters;

import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * A filter over a single partition.
 */
public class SlicePartitionFilter implements PartitionFilter
{
    private final PartitionColumns selectedColumns;
    private final Slices slices;
    private final boolean reversed;

    public SlicePartitionFilter(PartitionColumns columns, Slices slices, boolean reversed)
    {
        this.selectedColumns = columns;
        this.slices = slices;
        this.reversed = reversed;
    }

    public PartitionColumns selectedColumns()
    {
        return selectedColumns;
    }

    public boolean isReversed()
    {
        return reversed;
    }

    public boolean selectsAllPartition()
    {
        return !slices.hasLowerBound() && !slices.hasUpperBound();
    }

    public SlicePartitionFilter withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart)
    {
        Slices newSlices = slices.withUpdatedStart(comparator, newStart);
        return slices == newSlices
             ? this
             : new SlicePartitionFilter(selectedColumns, newSlices, reversed);
    }

    public SlicePartitionFilter withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
    {
        Slices newSlices = slices.withUpdatedEnd(comparator, newEnd);
        return slices == newSlices
             ? this
             : new SlicePartitionFilter(selectedColumns, newSlices, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition, DataLimits limits, int nowInSec)
    {
        // 'partition' represents the head of a partition. It covers what this filter requests if
        // we have one of the following condition:
        //   1) this filter requests the head of the partition and we request less than what
        //      the cache contains.
        //   2) the start and finish bounds of this filter are included in the cache.
        if (isHeadFilter() && limits.hasEnoughLiveData(partition, nowInSec))
            return true;

        // Note that since partition is the head of a partition, to have no lower bound is ok
        if (!slices.hasUpperBound() || partition.isEmpty())
            return false;

        Atom last = partition.tail();

        return partition.metadata().comparator.compare(slices.upperBound(), last.clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return !reversed && slices.size() == 1 && !slices.hasLowerBound();
    }

    // Given another iterator, only return the atoms that match this filter
    public AtomIterator filter(AtomIterator iterator)
    {
        final Slices.InOrderTester tester = slices.inOrderTester(reversed);

        FilteringRow row = new FilteringRow()
        {
            protected boolean include(ColumnDefinition column)
            {
                return selectedColumns.contains(column);
            }
        };

        // Note that we don't filter markers because that's a bit trickier (we don't know in advance until when
        // the range extend) and it's harmless to left them.
        return new RowFilteringAtomIterator(iterator, row)
        {
            @Override
            protected boolean includeRow(Row row)
            {
                return tester.includes(row.clustering());
            }

            @Override
            public boolean hasNext()
            {
                return !tester.isDone() && super.hasNext();
            }
        };
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, DecoratedKey key)
    {
        if (reversed)
            // TODO
            throw new UnsupportedOperationException();

        return slices.makeSliceIterator(new SSTableIterator(sstable, key, selectedColumns));
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        if (reversed)
            // TODO
            throw new UnsupportedOperationException();

        return slices.makeSliceIterator(new SSTableIterator(sstable, file, key, selectedColumns, indexEntry));
    }

    public AtomIterator getAtomIterator(Partition partition)
    {
        return partition.atomIterator(selectedColumns, slices, reversed);
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;

        if (minClusteringValues.isEmpty() || maxClusteringValues.isEmpty())
            return true;

        return slices.intersects(minClusteringValues, maxClusteringValues);
    }

    public String toString(CFMetaData metadata)
    {
        return String.format("slice(%s, slices=%s, reversed=%b)", selectedColumns, slices, reversed);
    }

    // From SliceQueryFilter
    //public static class Serializer implements IVersionedSerializer<SliceQueryFilter>
    //{
    //    private CType type;

    //    public Serializer(CType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serialize(SliceQueryFilter f, DataOutputPlus out, int version) throws IOException
    //    {
    //        out.writeInt(f.slices.length);
    //        for (ColumnSlice slice : f.slices)
    //            type.sliceSerializer().serialize(slice, out, version);
    //        out.writeBoolean(f.reversed);
    //        int count = f.count;
    //        out.writeInt(count);

    //        out.writeInt(f.compositesToGroup);
    //    }

    //    public SliceQueryFilter deserialize(DataInput in, int version) throws IOException
    //    {
    //        ColumnSlice[] slices;
    //        slices = new ColumnSlice[in.readInt()];
    //        for (int i = 0; i < slices.length; i++)
    //            slices[i] = type.sliceSerializer().deserialize(in, version);
    //        boolean reversed = in.readBoolean();
    //        int count = in.readInt();
    //        int compositesToGroup = -1;
    //        compositesToGroup = in.readInt();

    //        return new SliceQueryFilter(slices, reversed, count, compositesToGroup);
    //    }

    //    public long serializedSize(SliceQueryFilter f, int version)
    //    {
    //        TypeSizes sizes = TypeSizes.NATIVE;

    //        int size = 0;
    //        size += sizes.sizeof(f.slices.length);
    //        for (ColumnSlice slice : f.slices)
    //            size += type.sliceSerializer().serializedSize(slice, version);
    //        size += sizes.sizeof(f.reversed);
    //        size += sizes.sizeof(f.count);

    //        size += sizes.sizeof(f.compositesToGroup);
    //        return size;
    //    }
    //}

    // From IDiskAtomFilter
    //public static class Serializer implements IVersionedSerializer<IDiskAtomFilter>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serialize(IDiskAtomFilter filter, DataOutputPlus out, int version) throws IOException
    //    {
    //        if (filter instanceof SliceQueryFilter)
    //        {
    //            out.writeByte(0);
    //            type.sliceQueryFilterSerializer().serialize((SliceQueryFilter)filter, out, version);
    //        }
    //        else
    //        {
    //            out.writeByte(1);
    //            type.namesQueryFilterSerializer().serialize((NamesQueryFilter)filter, out, version);
    //        }
    //    }

    //    public IDiskAtomFilter deserialize(DataInput in, int version) throws IOException
    //    {
    //        int b = in.readByte();
    //        if (b == 0)
    //        {
    //            return type.sliceQueryFilterSerializer().deserialize(in, version);
    //        }
    //        else
    //        {
    //            assert b == 1;
    //            return type.namesQueryFilterSerializer().deserialize(in, version);
    //        }
    //    }

    //    public long serializedSize(IDiskAtomFilter filter, int version)
    //    {
    //        int size = 1;
    //        if (filter instanceof SliceQueryFilter)
    //            size += type.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)filter, version);
    //        else
    //            size += type.namesQueryFilterSerializer().serializedSize((NamesQueryFilter)filter, version);
    //        return size;
    //    }
    //}

}
