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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SimpleAbstractColumnIterator;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

public class CollationController extends AbstractCollationController
{
    private int sstablesIterated = 0;

    public CollationController(ColumnFamilyStore cfs, boolean mutableColumns, QueryFilter filter, int gcBefore)
    {
        super(cfs, mutableColumns, filter, gcBefore);
    }

    protected ColumnFamilyStore.ViewFragment getView()
    {
        return cfs.markReferenced(filter.key);
    }

    protected void addTimeOrderedData(ColumnFamilyStore.ViewFragment view, ColumnFamily container, List<OnDiskAtomIterator> iterators, QueryFilter reducedFilter)
    {
        /* add the SSTables on disk */
        Collections.sort(view.sstables, SSTable.maxTimestampComparator);

        // read sorted sstables
        long mostRecentRowTombstone = Long.MIN_VALUE;
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a row tombstone with a timestamp greater 
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (sstable.getMaxTimestamp() < mostRecentRowTombstone)
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            reduceNameFilter(reducedFilter, container, currentMaxTs);
            if (((NamesQueryFilter) reducedFilter.filter).columns.isEmpty())
                break;

            OnDiskAtomIterator iter = reducedFilter.getSSTableColumnIterator(sstable);
            iterators.add(iter);
            if (iter.getColumnFamily() != null)
            {
                ColumnFamily cf = iter.getColumnFamily();
                if (cf.isMarkedForDelete())
                {
                    // track the most recent row level tombstone we encounter
                    mostRecentRowTombstone = cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt;
                }

                container.delete(cf);
                sstablesIterated++;
                while (iter.hasNext())
                    container.addAtom(iter.next());
            }
        }
    }

    protected void postTimeOrderedQuery(ColumnFamily returnCF)
    {

        // "hoist up" the requested data into a more recent sstable
        if (sstablesIterated > cfs.getMinimumCompactionThreshold()
                && !cfs.isCompactionDisabled()
                && cfs.getCompactionStrategy() instanceof SizeTieredCompactionStrategy)
        {
            RowMutation rm = new RowMutation(cfs.table.name, new Row(filter.key, returnCF.cloneMe()));
            try
            {
                // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                Table.open(rm.getTable()).apply(rm, false, false);
            }
            catch (IOException e)
            {
                // log and allow the result to be returned
                logger.error("Error re-writing read results", e);
            }
        }
    }

    protected void addData(ColumnFamilyStore.ViewFragment view, ColumnFamily returnCF, List<OnDiskAtomIterator> iterators)
    {
        long mostRecentRowTombstone = Long.MIN_VALUE;
        Map<OnDiskAtomIterator, Long> iteratorMaxTimes = Maps.newHashMapWithExpectedSize(view.sstables.size());
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a row tombstone with a timestamp greater 
            // than the most recent update to this sstable, we can skip it
            if (sstable.getMaxTimestamp() < mostRecentRowTombstone)
                continue;

            OnDiskAtomIterator iter = filter.getSSTableColumnIterator(sstable);
            iteratorMaxTimes.put(iter, sstable.getMaxTimestamp());
            if (iter.getColumnFamily() != null)
            {
                ColumnFamily cf = iter.getColumnFamily();
                if (cf.isMarkedForDelete())
                    mostRecentRowTombstone = cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt;

                returnCF.delete(cf);
                sstablesIterated++;
            }
        }

        // If we saw a row tombstone, do a second pass through the iterators we
        // obtained from the sstables and drop any whose maxTimestamp < that of the
        // row tombstone
        for (Map.Entry<OnDiskAtomIterator, Long> entry : iteratorMaxTimes.entrySet())
        {
            if (entry.getValue() >= mostRecentRowTombstone)
                iterators.add(entry.getKey());
        }
    }

    public int getSstablesIterated()
    {
        return sstablesIterated;
    }
}
