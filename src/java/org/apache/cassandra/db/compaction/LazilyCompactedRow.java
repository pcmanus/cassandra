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
package org.apache.cassandra.db.compaction;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ColumnNameHelper;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * LazilyCompactedRow only computes the row bloom filter and column index in memory
 * (at construction time); it does this by reading one column at a time from each
 * of the rows being compacted, and merging them as it does so.  So the most we have
 * in memory at a time is the bloom filter, the index, and one column from each
 * pre-compaction row.
 */
public class LazilyCompactedRow extends AbstractCompactedRow
{
    private final List<? extends OnDiskAtomIterator> rows;
    private final CompactionController controller;
    private final long maxPurgeableTimestamp;
    private final ColumnFamily emptyColumnFamily;
    private ColumnStats columnStats;
    private boolean closed;
    private ColumnIndex.Builder indexBuilder;
    private final SecondaryIndexManager.Updater indexer;
    private final Reducer reducer;
    private final Iterator<OnDiskAtom> merger;

    public LazilyCompactedRow(CompactionController controller, List<? extends OnDiskAtomIterator> rows)
    {
        super(rows.get(0).getKey());
        this.rows = rows;
        this.controller = controller;
        indexer = controller.cfs.indexManager.updaterFor(key);

        ColumnFamily rawCf = null;
        for (OnDiskAtomIterator row : rows)
        {
            ColumnFamily cf = row.getColumnFamily();

            if (rawCf == null)
                rawCf = cf;
            else
                rawCf.delete(cf);
        }
        maxPurgeableTimestamp = controller.maxPurgeableTimestamp(key);
        // even if we can't delete all the tombstones allowed by gcBefore, we should still call removeDeleted
        // to get rid of redundant row-level and range tombstones
        assert rawCf != null;
        int overriddenGcBefore = rawCf.deletionInfo().maxTimestamp() < maxPurgeableTimestamp ? controller.gcBefore : Integer.MIN_VALUE;
        ColumnFamily purgedCf = ColumnFamilyStore.removeDeleted(rawCf, overriddenGcBefore);
        emptyColumnFamily = purgedCf == null ? ArrayBackedSortedColumns.factory.create(controller.cfs.metadata) : purgedCf;

        reducer = new Reducer();
        merger = Iterators.filter(MergeIterator.get(rows, emptyColumnFamily.getComparator().onDiskAtomComparator(), reducer), Predicates.notNull());
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey key, boolean shouldPurge, CompactionController controller, ColumnFamily cf)
    {
        // We should only gc tombstone if shouldPurge == true. But otherwise,
        // it is still ok to collect column that shadowed by their (deleted)
        // container, which removeDeleted(cf, Integer.MAX_VALUE) will do
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf,
                                                                 shouldPurge ? controller.gcBefore : Integer.MIN_VALUE,
                                                                 controller.cfs.indexManager.updaterFor(key));
        if (shouldPurge && compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
            CounterColumn.mergeAndRemoveOldShards(key, compacted, controller.gcBefore, controller.mergeShardBefore);
        return compacted;
    }

    public RowIndexEntry write(long currentPosition, DataOutput out) throws IOException
    {
        assert !closed;

        ColumnIndex columnsIndex;
        try
        {
            indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, out);
            columnsIndex = indexBuilder.buildForCompaction(merger);
            if (columnsIndex.columnsIndex.isEmpty())
            {
                boolean cfIrrelevant = emptyColumnFamily.deletionInfo().maxTimestamp() < maxPurgeableTimestamp
                                     ? ColumnFamilyStore.removeDeletedCF(emptyColumnFamily, controller.gcBefore) == null
                                     : !emptyColumnFamily.isMarkedForDelete(); // tombstones are relevant
                if (cfIrrelevant)
                    return null;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // reach into the reducer (created during iteration) to get column count, size, max column timestamp
        columnStats = new ColumnStats(reducer.columns,
                                      reducer.minTimestampSeen,
                                      Math.max(emptyColumnFamily.deletionInfo().maxTimestamp(), reducer.maxTimestampSeen),
                                      reducer.maxLocalDeletionTimeSeen,
                                      reducer.tombstones,
                                      reducer.minColumnNameSeen,
                                      reducer.maxColumnNameSeen
        );

        indexBuilder.maybeWriteEmptyRowHeader();
        out.writeShort(SSTableWriter.END_OF_ROW);

        close();

        return RowIndexEntry.create(currentPosition, emptyColumnFamily.deletionInfo().getTopLevelDeletion(), columnsIndex);
    }

    public void update(MessageDigest digest)
    {
        assert !closed;

        // no special-case for rows.size == 1, we're actually skipping some bytes here so just
        // blindly updating everything wouldn't be correct
        DataOutputBuffer out = new DataOutputBuffer();

        try
        {
            DeletionTime.serializer.serialize(emptyColumnFamily.deletionInfo().getTopLevelDeletion(), out);
            digest.update(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        // initialize indexBuilder for the benefit of its tombstoneTracker, used by our reducing iterator
        indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, out);
        while (merger.hasNext())
            merger.next().updateDigest(digest);
        close();
    }

    public ColumnStats columnStats()
    {
        return columnStats;
    }

    public void close()
    {
        for (OnDiskAtomIterator row : rows)
        {
            try
            {
                row.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        closed = true;
    }

    private class Reducer extends MergeIterator.Reducer<OnDiskAtom, OnDiskAtom>
    {
        // all columns reduced together will have the same name, so there will only be one column
        // in the container; we just want to leverage the conflict resolution code from CF
        ColumnFamily container = emptyColumnFamily.cloneMeShallow(ArrayBackedSortedColumns.factory, false);

        // tombstone reference; will be reconciled w/ column during getReduced
        RangeTombstone tombstone;

        int columns = 0;
        long minTimestampSeen = Long.MAX_VALUE;
        long maxTimestampSeen = Long.MIN_VALUE;
        int maxLocalDeletionTimeSeen = Integer.MIN_VALUE;
        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
        List<ByteBuffer> minColumnNameSeen = Collections.emptyList();
        List<ByteBuffer> maxColumnNameSeen = Collections.emptyList();

        public void reduce(OnDiskAtom current)
        {
            if (current instanceof RangeTombstone)
            {
                tombstone = (RangeTombstone)current;
            }
            else
            {
                Column column = (Column) current;
                container.addColumn(column);

                // skip the index-update checks if there is no indexing needed since they are a bit expensive
                if (indexer == SecondaryIndexManager.nullUpdater)
                    return;

                if (!column.isMarkedForDelete(System.currentTimeMillis())
                    && !container.getColumn(column.name()).equals(column))
                {
                    indexer.remove(column);
                }
            }
        }

        protected OnDiskAtom getReduced()
        {
            if (tombstone != null)
            {
                RangeTombstone t = tombstone;
                tombstone = null;

                if (t.data.isGcAble(controller.gcBefore))
                {
                    return null;
                }
                else
                {
                    return t;
                }
            }
            else
            {
                boolean shouldPurge = container.getSortedColumns().iterator().next().timestamp() < maxPurgeableTimestamp;
                ColumnFamily purged = removeDeletedAndOldShards(key, shouldPurge, controller, container);
                if (purged == null || !purged.iterator().hasNext())
                {
                    container.clear();
                    return null;
                }
                Column reduced = purged.iterator().next();
                container.clear();

                // removeDeletedAndOldShards have only checked the top-level CF deletion times,
                // not the range tombstone. For that we use the columnIndexer tombstone tracker.
                if (indexBuilder.tombstoneTracker().isDeleted(reduced))
                {
                    indexer.remove(reduced);
                    return null;
                }

                columns++;
                minTimestampSeen = Math.min(minTimestampSeen, reduced.minTimestamp());
                maxTimestampSeen = Math.max(maxTimestampSeen, reduced.maxTimestamp());
                maxLocalDeletionTimeSeen = Math.max(maxLocalDeletionTimeSeen, reduced.getLocalDeletionTime());
                minColumnNameSeen = ColumnNameHelper.minComponents(minColumnNameSeen, reduced.name(), controller.cfs.metadata.comparator);
                maxColumnNameSeen = ColumnNameHelper.maxComponents(maxColumnNameSeen, reduced.name(), controller.cfs.metadata.comparator);

                int deletionTime = reduced.getLocalDeletionTime();
                if (deletionTime < Integer.MAX_VALUE)
                {
                    tombstones.update(deletionTime);
                }
                return reduced;
            }
        }
    }
}
