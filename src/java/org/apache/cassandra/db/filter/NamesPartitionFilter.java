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
package org.apache.cassandra.db.filter;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.SearchIterator;

/**
 * A filter selecting rows given their clustering value.
 */
public class NamesPartitionFilter extends AbstractPartitionFilter
{
    static final InternalDeserializer deserializer = new NamesDeserializer();

    // This could be empty if selectedColums only has static columns (in which case the filter still
    // selects the static row)
    private final SortedSet<Clustering> clusterings;

    // clusterings is always in clustering order (because we need it that way in some methods), but we also
    // sometimes need those clustering in "query" order (i.e. in reverse clustering order if the query is
    // reversed), so we keep that too for simplicity.
    private final SortedSet<Clustering> clusteringsInQueryOrder;

    public NamesPartitionFilter(ColumnsSelection columns, SortedSet<Clustering> clusterings, boolean reversed)
    {
        super(Kind.NAMES, columns, reversed);
        assert !clusterings.isEmpty() || !columns.columns().statics.isEmpty();
        assert !clusterings.contains(Clustering.STATIC_CLUSTERING);
        this.clusterings = clusterings;
        this.clusteringsInQueryOrder = reversed ? reverse(clusterings) : clusterings;
    }

    public NamesPartitionFilter(PartitionColumns columns, SortedSet<Clustering> clusterings, boolean reversed)
    {
        this(ColumnsSelection.withoutSubselection(columns), clusterings, reversed);
    }

    private static SortedSet<Clustering> reverse(SortedSet<Clustering> set)
    {
        SortedSet<Clustering> reversed = new TreeSet<>(Collections.reverseOrder(set.comparator()));
        reversed.addAll(set);
        return reversed;
    }

    /**
     * The set of requested rows.
     *
     * Please note that this can be empty if only the static row is requested.
     *
     * @return the set of requested clustering in clustering order (note that
     * this is always in clustering order even if the query is reversed).
     */
    public SortedSet<Clustering> requestedRows()
    {
        return clusterings;
    }

    public boolean selectsAllPartition()
    {
        return false;
    }

    public boolean selects(Clustering clustering)
    {
        return clustering == Clustering.STATIC_CLUSTERING
             ? !queriedColumns.columns().statics.isEmpty()
             : clusterings.contains(clustering);
    }

    public NamesPartitionFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive)
    {
        int cmp = comparator.compare(lastReturned, clusteringsInQueryOrder.first());
        if (cmp < 0 || (inclusive && cmp == 0))
            return this;

        SortedSet<Clustering> newClusterings;
        if (reversed)
        {
            newClusterings = new TreeSet<>(clusterings.headSet(lastReturned));
            if (!inclusive && comparator.compare(newClusterings.last(), lastReturned) == 0)
                newClusterings.remove(lastReturned);
        }
        else
        {
            newClusterings = new TreeSet<>(clusterings.tailSet(lastReturned));
            if (!inclusive && comparator.compare(newClusterings.first(), lastReturned) == 0)
                newClusterings.remove(lastReturned);
        }

        return new NamesPartitionFilter(queriedColumns, newClusterings, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        // 'partition' contains all columns, so it covers our filter if our last clusterings
        // is smaller than the last in the cache
        return ((ClusteringComparator)clusterings.comparator()).compare(clusterings.last(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return false;
    }

    // Given another iterator, only return the atoms that match this filter
    public AtomIterator filter(AtomIterator iterator)
    {
        // Note that we don't filter markers because that's a bit trickier (we don't know in advance until when
        // the range extend) and it's harmless to left them.
        return new RowFilteringAtomIterator(iterator)
        {
            @Override
            public FilteringRow makeRowFilter()
            {
                return FilteringRow.columnsFilteringRow(queriedColumns);
            }

            @Override
            protected boolean includeRow(Row row)
            {
                return clusterings.contains(row.clustering());
            }
        };
    }

    public AtomIterator filter(final SliceableAtomIterator iter)
    {
        return new WrappingAtomIterator(iter)
        {
            private final Iterator<Clustering> clusteringIter = clusteringsInQueryOrder.iterator();
            private Atom next;

            @Override
            public boolean hasNext()
            {
                if (next != null)
                    return true;

                while (clusteringIter.hasNext())
                {
                    Clustering nextClustering = clusteringIter.next();
                    Iterator<Atom> sliceIter = iter.slice(Slice.make(metadata().comparator, nextClustering));
                    if (sliceIter.hasNext())
                    {
                        next = sliceIter.next();
                        assert !sliceIter.hasNext();
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

    public AtomIterator getAtomIterator(final Partition partition, final int nowInSec)
    {
        final SearchIterator<Clustering, Row> searcher = partition.searchIterator(queriedColumns, reversed, nowInSec);
        return new AbstractAtomIterator(partition.metadata(),
                                        partition.partitionKey(),
                                        partition.partitionLevelDeletion(),
                                        queriedColumns.columns(),
                                        searcher.next(Clustering.STATIC_CLUSTERING),
                                        reversed,
                                        partition.stats(),
                                        nowInSec)
        {
            private final Iterator<Clustering> clusteringIter = clusteringsInQueryOrder.iterator();

            protected Atom computeNext()
            {
                while (clusteringIter.hasNext() && searcher.hasNext())
                {
                    Row row = searcher.next(clusteringIter.next());
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
        // Note that empty clustering still mean we're querying the static row.
        if (clusterings.isEmpty())
            return countCells ? queriedColumns.columns().statics.columnCount() : 1;

        return countCells ? clusterings.size() * queriedColumns.columns().regulars.columnCount() : clusterings.size();
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("names(").append(queriedColumns).append(", {");
        int i = 0;
        for (Clustering clustering : clusterings)
            sb.append(i++ == 0 ? "" : ", ").append(clustering.toString(metadata));
        if (reversed)
            sb.append(", reversed");
        return sb.append("}").toString();
    }

    public String toCQLString(CFMetaData metadata)
    {
        if (clusterings.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append("(").append(ColumnDefinition.toCQLString(metadata.clusteringColumns())).append(")");
        sb.append(clusterings.size() == 1 ? " = " : " IN (");
        int i = 0;
        for (Clustering clustering : clusterings)
            sb.append(i++ == 0 ? "" : ", ").append("(").append(clustering.toCQLString(metadata)).append(")");
        sb.append(clusterings.size() == 1 ? "" : ")");

        appendOrderByToCQLString(metadata, sb);
        return sb.toString();
    }

    protected void serializeInternal(DataOutputPlus out, int version) throws IOException
    {
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        out.writeInt(clusterings.size());
        for (Clustering clustering : clusterings)
            Clustering.serializer.serialize(clustering, out, version, comparator.subtypes());
    }

    protected long serializedSizeInternal(int version, TypeSizes sizes)
    {
        long size = 0;
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        for (Clustering clustering : clusterings)
            size += Clustering.serializer.serializedSize(clustering, version, comparator.subtypes(), sizes);
        return size;
    }

    private static class NamesDeserializer extends InternalDeserializer
    {
        public PartitionFilter deserialize(DataInput in, int version, CFMetaData metadata, ColumnsSelection columns, boolean reversed) throws IOException
        {
            ClusteringComparator comparator = metadata.comparator;
            SortedSet<Clustering> clusterings = new TreeSet(comparator);
            int size = in.readInt();
            for (int i = 0; i < size; i++)
                clusterings.add(Clustering.serializer.deserialize(in, version, comparator.subtypes()).takeAlias());

            return new NamesPartitionFilter(columns, clusterings, reversed);
        }
    }
}
