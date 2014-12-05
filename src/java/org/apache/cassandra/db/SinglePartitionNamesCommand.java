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

import java.util.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionNamesCommand extends SinglePartitionReadCommand<NamesPartitionFilter>
{
    public SinglePartitionNamesCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       NamesPartitionFilter partitionFilter)
    {
        super(metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    public SinglePartitionNamesCommand copy()
    {
        return new SinglePartitionNamesCommand(metadata(), nowInSec(), columnFilter(), limits(), partitionKey(), partitionFilter());
    }

    public SinglePartitionReadCommand maybeGenerateRetryCommand(DataResolver resolver, Row row)
    {
        throw new UnsupportedOperationException();
    }

    protected AtomIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(partitionKey()));

        ArrayBackedPartition result = null;
        NamesPartitionFilter filter = partitionFilter();

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey());
            if (partition == null)
                continue;

            AtomIterator iter = filter.getAtomIterator(partition);
            if (AtomIterators.isEmpty(iter))
                continue;

            if (copyOnHeap)
                iter = AtomIterators.cloningIterator(iter, HeapAllocator.instance);
            result = add(iter, result);
        }

        /* add the SSTables on disk */
        Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
        int sstablesIterated = 0;

        // read sorted sstables
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a row tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);
            if (filter == null)
                break;

            Tracing.trace("Merging data from sstable {}", sstable.descriptor.generation);
            AtomIterator iter = filter.getSSTableAtomIterator(sstable, partitionKey());
            if (AtomIterators.isEmpty(iter))
                continue;

            sstablesIterated++;
            result = add(iter, result);
        }

        cfs.metric.updateSSTableIterated(sstablesIterated);

        if (result == null || result.isEmpty())
            return AtomIterators.emptyIterator(metadata(), partitionKey(), false);

        // "hoist up" the requested data into a more recent sstable
        if (sstablesIterated > cfs.getMinimumCompactionThreshold()
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategy() instanceof SizeTieredCompactionStrategy)
        {
            Tracing.trace("Defragmenting requested data");
            Mutation mutation = new Mutation(AtomIterators.toUpdate(result.atomIterator(result.columns(), Slices.ALL, false)));
            // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
            Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
        }

        return result.atomIterator(result.columns(), Slices.ALL, false);
    }

    private ArrayBackedPartition add(AtomIterator iter, ArrayBackedPartition result)
    {
        int maxRows = partitionFilter().maxQueried(false);
        if (result == null)
            return ArrayBackedPartition.create(iter, maxRows);

        AtomIterator merged = AtomIterators.merge(Arrays.asList(iter, result.atomIterator(result.columns(), Slices.ALL, false)), nowInSec());
        return ArrayBackedPartition.create(merged, maxRows);
    }

    private NamesPartitionFilter reduceFilter(NamesPartitionFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<ClusteringPrefix, Row> searchIter = result.searchIterator(result.columns());
        SortedSet<ClusteringPrefix> toRemove = null;

        // We remove rows if we have values for all the requested columns. TODO: we could also remove a selected column
        // if we've found values for every requested row.
        for (ClusteringPrefix clustering : filter.requestedRows())
        {
            if (!searchIter.hasNext())
                break;

            Row row = searchIter.next(clustering);
            // TODO: if we allow static in NamesPartitionFilter, we should update this!!
            if (row == null || !canRemoveRow(row, filter.selectedColumns().regulars, sstableTimestamp))
                continue;

            if (toRemove == null)
                toRemove = new TreeSet<>(result.metadata().comparator);
            toRemove.add(clustering);
        }

        if (toRemove == null)
            return filter;

        // Check if we have everything we need
        if (toRemove.size() == filter.requestedRows().size())
            return null;

        SortedSet<ClusteringPrefix> newColumns = new TreeSet<>(result.metadata().comparator);
        newColumns.addAll(Sets.difference(filter.requestedRows(), toRemove));
        return new NamesPartitionFilter(filter.selectedColumns(), newColumns);
    }

    private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        for (ColumnDefinition column : requestedColumns)
        {
            // We can never be sure we have all the collection and so CQL should never use this code
            // path if a collection is queried.
            assert !column.type.isCollection();
            if (column.type.isCollection())
                return false;

            Cell cell = row.getCell(column);
            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }
        return true;
    }
}
