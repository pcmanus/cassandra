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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * A filter over a single partition.
 */
public class NamesPartitionFilter implements PartitionFilter
{
    private final Columns selectedColumns;
    private final Columns selectedStaticColumns;
    private final SortedSet<ClusteringPrefix> prefixes;

    public NamesPartitionFilter(Columns columns, Columns staticColumns, SortedSet<ClusteringPrefix> prefixes)
    {
        this.selectedColumns = columns;
        this.selectedStaticColumns = staticColumns;
        this.prefixes = prefixes;
    }

    public Columns selectedColumns()
    {
        return selectedColumns;
    }

    public Columns selectedStaticColumns()
    {
        return selectedStaticColumns;
    }

    public boolean selectsAllPartition()
    {
        return false;
    }

    public boolean isReversed()
    {
        return false;
    }

    public PartitionFilter withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart)
    {
        if (comparator.compare(newStart, prefixes.first()) <= 0)
            return this;
        else
            return new NamesPartitionFilter(selectedColumns, selectedStaticColumns, prefixes.tailSet(newStart));
    }

    public PartitionFilter withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
    {
        if (comparator.compare(prefixes.last(), newEnd) <= 0)
            return this;
        else
            return new NamesPartitionFilter(selectedColumns, selectedStaticColumns, prefixes.headSet(newEnd));
    }

    public boolean isFullyCoveredBy(CachedPartition partition, DataLimits limits, int nowInSec)
    {
        // 'partition' contains all the columns, so it covers everything we query if our last prefixes
        // is smaller than the last in the cache
        return prefixes.comparator().compare(prefixes.last().clustering(), partition.tail().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return false;
    }

    public AtomIterator filter(Partition partition)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // Given another iterator, only return the atoms that match this filter
    public AtomIterator filter(AtomIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
        //return new AbstractFilteringIterator(iterator)
        //{
        //    private boolean isDone;

        //    protected abstract RangeTombstone filter(RangeTombstone rt)
        //    {
        //        return tester.intersects(rt.min(), rt.max()) ? rt : null;
        //    }

        //    protected abstract Row filter(Row row)
        //    {
        //        return tester.includes(row.clustering()) ? row : null;
        //    }

        //    @Override
        //    protected boolean isDone()
        //    {
        //        return tester.isDone();
        //    }
        //};
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableNamesIterator(sstable, key, selectedColumns, selectedStaticColumns, prefixes);
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableNamesIterator(sstable, file, key, selectedColumns, selectedStaticColumns, prefixes, indexEntry);
    }

    public AtomIterator getAtomIterator(Partition partition)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        // TODO: we could actually exclude some sstables
        return true;
    }

    public int maxQueried(boolean countCells)
    {
        return countCells ? prefixes.size() * selectedColumns.size() : prefixes.size();
    }

    // From NamesQueryFilter
    //public static class Serializer implements IVersionedSerializer<NamesQueryFilter>
    //{
    //    private CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serialize(NamesQueryFilter f, DataOutputPlus out, int version) throws IOException
    //    {
    //        out.writeInt(f.columns.size());
    //        ISerializer<CellName> serializer = type.cellSerializer();
    //        for (CellName cName : f.columns)
    //        {
    //            serializer.serialize(cName, out);
    //        }
    //        out.writeBoolean(f.countCQL3Rows);
    //    }

    //    public NamesQueryFilter deserialize(DataInput in, int version) throws IOException
    //    {
    //        int size = in.readInt();
    //        SortedSet<CellName> columns = new TreeSet<>(type);
    //        ISerializer<CellName> serializer = type.cellSerializer();
    //        for (int i = 0; i < size; ++i)
    //            columns.add(serializer.deserialize(in));
    //        boolean countCQL3Rows = in.readBoolean();
    //        return new NamesQueryFilter(columns, countCQL3Rows);
    //    }

    //    public long serializedSize(NamesQueryFilter f, int version)
    //    {
    //        TypeSizes sizes = TypeSizes.NATIVE;
    //        int size = sizes.sizeof(f.columns.size());
    //        ISerializer<CellName> serializer = type.cellSerializer();
    //        for (CellName cName : f.columns)
    //            size += serializer.serializedSize(cName, sizes);
    //        size += sizes.sizeof(f.countCQL3Rows);
    //        return size;
    //    }
    //}
}
