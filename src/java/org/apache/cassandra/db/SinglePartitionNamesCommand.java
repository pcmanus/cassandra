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

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.ColumnFamilyMetrics.Sampler;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionNamesCommand extends SinglePartitionReadCommand<NamesPartitionFilter>
{
    protected SinglePartitionNamesCommand(boolean isDigest,
                                          boolean isForThrift,
                                          CFMetaData metadata,
                                          int nowInSec,
                                          ColumnFilter columnFilter,
                                          DataLimits limits,
                                          DecoratedKey partitionKey,
                                          NamesPartitionFilter partitionFilter)
    {
        super(isDigest, isForThrift, metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    public SinglePartitionNamesCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       NamesPartitionFilter partitionFilter)
    {
        this(false, false, metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    public SinglePartitionNamesCommand copy()
    {
        return new SinglePartitionNamesCommand(isDigestQuery(), isForThrift(), metadata(), nowInSec(), columnFilter(), limits(), partitionKey(), partitionFilter());
    }

    protected UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
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

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(partition, nowInSec()))
            {
                if (iter.isEmpty())
                    continue;

                UnfilteredRowIterator clonedFilter = copyOnHeap
                                          ? UnfilteredRowIterators.cloningIterator(iter, HeapAllocator.instance)
                                          : iter;
                result = add(isForThrift() ? ThriftResultsMerger.maybeWrap(clonedFilter) : clonedFilter, result);
            }
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
            sstable.incrementReadCount();
            try (UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), filter.queriedColumns(), filter.isReversed(), nowInSec(), isForThrift())))
            {
                if (iter.isEmpty())
                    continue;

                sstablesIterated++;
                result = add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter) : iter, result);
            }
        }

        cfs.metric.updateSSTableIterated(sstablesIterated);

        if (result == null || result.isEmpty())
            return UnfilteredRowIterators.emptyIterator(metadata(), partitionKey(), false, nowInSec());

        DecoratedKey key = result.partitionKey();
        cfs.metric.samplers.get(Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);

        // "hoist up" the requested data into a more recent sstable
        if (sstablesIterated > cfs.getMinimumCompactionThreshold()
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategy().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            try (UnfilteredRowIterator iter = result.unfilteredIterator(queriedColumns(), Slices.ALL, false, nowInSec()))
            {
                final Mutation mutation = new Mutation(UnfilteredRowIterators.toUpdate(iter));
                StageManager.getStage(Stage.MUTATION).execute(new Runnable()
                {
                    public void run()
                    {
                        // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                        Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
                    }
                });
            }
        }

        return result.unfilteredIterator(queriedColumns(), Slices.ALL, partitionFilter().isReversed(), nowInSec());
    }

    private ArrayBackedPartition add(UnfilteredRowIterator iter, ArrayBackedPartition result)
    {
        int maxRows = partitionFilter().maxQueried(false);
        if (result == null)
            return ArrayBackedPartition.create(UnfilteredRowIterators.loggingIterator(iter, "result", true), maxRows);

        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(queriedColumns(), Slices.ALL, false, nowInSec())));
        return ArrayBackedPartition.create(merged, maxRows);
    }

    private NamesPartitionFilter reduceFilter(NamesPartitionFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(queriedColumns(), false, nowInSec());

        PartitionColumns columns = filter.queriedColumns().columns();
        SortedSet<Clustering> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with
        // both static and regular rows.
        // TODO: we could also remove a selected column if we've found values for every requested row but we'll leave
        // that for later.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = searchIter.next(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && canRemoveRow(staticRow, columns.statics, sstableTimestamp);
        }

        SortedSet<Clustering> toRemove = null;
        for (Clustering clustering : clusterings)
        {
            if (!searchIter.hasNext())
                break;

            Row row = searchIter.next(clustering);
            if (row == null || !canRemoveRow(row, columns.regulars, sstableTimestamp))
                continue;

            if (toRemove == null)
                toRemove = new TreeSet<>(result.metadata().comparator);
            toRemove.add(clustering);
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        PartitionColumns newColumns = columns;
        if (removeStatic)
            newColumns = new PartitionColumns(Columns.NONE, columns.regulars);

        SortedSet<Clustering> newClusterings = clusterings;
        if (toRemove != null)
        {
            newClusterings = new TreeSet<>(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
        }
        return new NamesPartitionFilter(newColumns, newClusterings, filter.isReversed());
    }

    private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        for (ColumnDefinition column : requestedColumns)
        {
            // We can never be sure we have all of a collection, so never remove rows in that case.
            if (column.type.isCollection())
                return false;

            Cell cell = row.getCell(column);
            if (cell == null || cell.livenessInfo().timestamp() <= sstableTimestamp)
                return false;
        }
        return true;
    }
}
