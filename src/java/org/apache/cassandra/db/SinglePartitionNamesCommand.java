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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.DataResolver;

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
        // TODO
        throw new UnsupportedOperationException();
        //final ColumnFamily container = ArrayBackedSortedColumns.factory.create(cfs.metadata, filter.filter.isReversed());
        //List<OnDiskAtomIterator> iterators = new ArrayList<>();
        //boolean isEmpty = true;
        //Tracing.trace("Acquiring sstable references");
        //ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(filter.key));

        //try
        //{
        //    Tracing.trace("Merging memtable contents");
        //    long mostRecentRowTombstone = Long.MIN_VALUE;
        //    for (Memtable memtable : view.memtables)
        //    {
        //        ColumnFamily cf = memtable.getColumnFamily(filter.key);
        //        if (cf != null)
        //        {
        //            filter.delete(container.deletionInfo(), cf);
        //            isEmpty = false;
        //            Iterator<Cell> iter = filter.getIterator(cf);
        //            while (iter.hasNext())
        //            {
        //                Cell cell = iter.next();
        //                if (copyOnHeap)
        //                    cell = cell.localCopy(cfs.metadata, HeapAllocator.instance);
        //                container.addColumn(cell);
        //            }
        //        }
        //        mostRecentRowTombstone = container.deletionInfo().getTopLevelDeletion().markedForDeleteAt;
        //    }

        //    // avoid changing the filter columns of the original filter
        //    // (reduceNameFilter removes columns that are known to be irrelevant)
        //    NamesQueryFilter namesFilter = (NamesQueryFilter) filter.filter;
        //    TreeSet<CellName> filterColumns = new TreeSet<>(namesFilter.columns);
        //    QueryFilter reducedFilter = new QueryFilter(filter.key, filter.cfName, namesFilter.withUpdatedColumns(filterColumns), filter.timestamp);

        //    /* add the SSTables on disk */
        //    Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);

        //    // read sorted sstables
        //    for (SSTableReader sstable : view.sstables)
        //    {
        //        // if we've already seen a row tombstone with a timestamp greater
        //        // than the most recent update to this sstable, we're done, since the rest of the sstables
        //        // will also be older
        //        if (sstable.getMaxTimestamp() < mostRecentRowTombstone)
        //            break;

        //        long currentMaxTs = sstable.getMaxTimestamp();
        //        reduceNameFilter(reducedFilter, container, currentMaxTs);
        //        if (((NamesQueryFilter) reducedFilter.filter).columns.isEmpty())
        //            break;

        //        Tracing.trace("Merging data from sstable {}", sstable.descriptor.generation);
        //        OnDiskAtomIterator iter = reducedFilter.getSSTableColumnIterator(sstable);
        //        iterators.add(iter);
        //        isEmpty = false;
        //        if (iter.getColumnFamily() != null)
        //        {
        //            container.delete(iter.getColumnFamily());
        //            sstablesIterated++;
        //            while (iter.hasNext())
        //                container.addAtom(iter.next());
        //        }
        //        mostRecentRowTombstone = container.deletionInfo().getTopLevelDeletion().markedForDeleteAt;
        //    }

        //    // we need to distinguish between "there is no data at all for this row" (BF will let us rebuild that efficiently)
        //    // and "there used to be data, but it's gone now" (we should cache the empty CF so we don't need to rebuild that slower)
        //    if (isEmpty)
        //        return null;

        //    // do a final collate.  toCollate is boilerplate required to provide a CloseableIterator
        //    ColumnFamily returnCF = container.cloneMeShallow();
        //    Tracing.trace("Collating all results");
        //    filter.collateOnDiskAtom(returnCF, container.iterator(), gcBefore);

        //    // "hoist up" the requested data into a more recent sstable
        //    if (sstablesIterated > cfs.getMinimumCompactionThreshold()
        //        && !cfs.isAutoCompactionDisabled()
        //        && cfs.getCompactionStrategy() instanceof SizeTieredCompactionStrategy)
        //    {
        //        Tracing.trace("Defragmenting requested data");
        //        Mutation mutation = new Mutation(cfs.keyspace.getName(), filter.key.getKey(), returnCF.cloneMe());
        //        // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
        //        Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
        //    }

        //    // Caller is responsible for final removeDeletedCF.  This is important for cacheRow to work correctly:
        //    return returnCF;
        //}
        //finally
        //{
        //    for (OnDiskAtomIterator iter : iterators)
        //        FileUtils.closeQuietly(iter);
        //}
    }

    //private void reduceNameFilter(QueryFilter filter, ColumnFamily container, long sstableTimestamp)
    //{
    //    if (container == null)
    //        return;

    //    SearchIterator<CellName, Cell> searchIter = container.searchIterator();
    //    for (Iterator<CellName> iterator = ((NamesQueryFilter) filter.filter).columns.iterator(); iterator.hasNext() && searchIter.hasNext(); )
    //    {
    //        CellName filterColumn = iterator.next();
    //        Cell cell = searchIter.next(filterColumn);
    //        if (cell != null && cell.timestamp() > sstableTimestamp)
    //            iterator.remove();
    //    }
    //}
}

