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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.SearchIterator;

/**
 * A filter over a single partition.
 */
public class NamesPartitionFilter implements PartitionFilter
{
    // TODO: we should either handle statics, or make it clear we don't (by rejecting the static prefix if provided)
    private final PartitionColumns selectedColumns;
    private final SortedSet<ClusteringPrefix> prefixes;

    public NamesPartitionFilter(PartitionColumns columns, SortedSet<ClusteringPrefix> prefixes)
    {
        this.selectedColumns = columns;
        this.prefixes = prefixes;
    }

    public PartitionColumns selectedColumns()
    {
        return selectedColumns;
    }

    public SortedSet<ClusteringPrefix> requestedRows()
    {
        return prefixes;
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
            return new NamesPartitionFilter(selectedColumns, prefixes.tailSet(newStart));
    }

    public PartitionFilter withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
    {
        if (comparator.compare(prefixes.last(), newEnd) <= 0)
            return this;
        else
            return new NamesPartitionFilter(selectedColumns, prefixes.headSet(newEnd));
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

    // Given another iterator, only return the atoms that match this filter
    public AtomIterator filter(AtomIterator iterator)
    {
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
                return prefixes.contains(row.clustering());
            }
        };
    }

    public static AtomIterator makeIterator(final SeekableAtomIterator iter, final SortedSet<ClusteringPrefix> clusterings)
    {
        assert !iter.isReverseOrder();
        return new WrappingAtomIterator(iter)
        {
            private final Iterator<ClusteringPrefix> prefixIter = clusterings.iterator();
            private Atom next;

            @Override
            public boolean hasNext()
            {
                if (next != null)
                    return true;

                while (prefixIter.hasNext())
                {
                    ClusteringPrefix nextPrefix = prefixIter.next();
                    if (iter.seekTo(nextPrefix, nextPrefix))
                    {
                        next = iter.next();
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Atom next()
            {
                if (next == null && !hasNext())
                    throw new NoSuchElementException();

                Atom toReturn = next;
                next = null;
                return toReturn;
            }
        };
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, DecoratedKey key)
    {
        return makeIterator(new SSTableIterator(sstable, key, selectedColumns), prefixes);
    }

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return makeIterator(new SSTableIterator(sstable, file, key, selectedColumns, indexEntry), prefixes);
    }

    public AtomIterator getAtomIterator(final Partition partition)
    {
        return new AbstractAtomIterator(partition.metadata(),
                                        partition.partitionKey(),
                                        partition.partitionLevelDeletion(),
                                        selectedColumns,
                                        Rows.EMPTY_STATIC_ROW,
                                        false,
                                        partition.stats())
        {
            private final Iterator<ClusteringPrefix> prefixIter = prefixes.iterator();
            private final SearchIterator<ClusteringPrefix, Row> searcher = partition.searchIterator(selectedColumns);

            protected Atom computeNext()
            {
                while (prefixIter.hasNext() && searcher.hasNext())
                {
                    Row row = searcher.next(prefixIter.next());
                    if (row != null)
                        return row;
                }
                return endOfData();
            }
        };
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        // TODO: we could actually exclude some sstables
        return true;
    }

    public int maxQueried(boolean countCells)
    {
        return countCells ? prefixes.size() * selectedColumns.regulars.columnCount() : prefixes.size();
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("names(").append(selectedColumns).append(", {");
        boolean isFirst = true;
        for (ClusteringPrefix prefix : prefixes)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            sb.append(prefix.toString(metadata));
        }
        return sb.append("}").toString();
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
