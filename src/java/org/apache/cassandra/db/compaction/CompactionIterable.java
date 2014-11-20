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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

// TODO: this should be merged with AbstractCompactionIterable (it's not the only implementation) and
// we should rename it as CompactionIterator
public class CompactionIterable extends AbstractCompactionIterable
{
    private static final long ATOMS_TO_UPDATE_PROGRESS = 100;

    private final PartitionIterator mergedIterator;
    private final AtomicInteger atomsMerged = new AtomicInteger();
    private final CompactionMetrics metrics;

    public CompactionIterable(OperationType type, List<ICompactionScanner> scanners, CompactionController controller)
    {
        this(type, scanners, controller, null);
    }

    public CompactionIterable(OperationType type, List<ICompactionScanner> scanners, CompactionController controller, CompactionMetrics metrics)
    {
        super(controller, type, scanners);
        this.mergedIterator = PurgingPartitionIterator.create(PartitionIterators.merge(scanners, FBUtilities.nowInSeconds(), listener()), controller);

        this.metrics = metrics;

        if (metrics != null)
            metrics.beginCompaction(this);
    }

    private PartitionIterators.MergeListener listener()
    {
        return new PartitionIterators.MergeListener()
        {
            public AtomIterators.MergeListener getAtomMergeListener(DecoratedKey partitionKey, AtomIterator[] versions)
            {
                int merged = 0;
                for (int i = 0; i < versions.length; i++)
                    if (versions[i] != null)
                        merged++;
                CompactionIterable.this.updateCounterFor(merged);

                /*
                 * The atom level listener does 2 things:
                 *  - It updates 2ndary indexes for deleted/shadowed cells
                 *  - It updates progress regularly (every ATOMS_TO_UPDATE_PROGRESS atoms)
                 */
                final SecondaryIndexManager.Updater indexer = controller.cfs.indexManager.gcUpdaterFor(partitionKey);
                return new AtomIterators.MergeListener()
                {
                    private ClusteringPrefix clustering;
                    private ColumnDefinition column;

                    public void onMergingRows(ClusteringPrefix clustering, long mergedTimestamp, Row[] versions)
                    {
                        this.clustering = clustering;
                    }

                    public void onMergedColumns(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTimeArray versions)
                    {
                        this.column = c;
                    }

                    public void onMergedCells(Cell mergedCell, Cell[] versions)
                    {
                        if (indexer == SecondaryIndexManager.nullUpdater)
                            return;

                        for (int i = 0; i < versions.length; i++)
                        {
                            Cell version = versions[i];
                            if (version != null && (mergedCell == null || !mergedCell.equals(version)))
                                indexer.remove(clustering, version);
                        }
                    }

                    public void onRowDone()
                    {
                        int merged = atomsMerged.incrementAndGet();
                        if (merged % ATOMS_TO_UPDATE_PROGRESS == 0)
                            updateBytesRead();
                    }

                    public void onMergedRangeTombstoneMarkers(ClusteringPrefix prefix, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions)
                    {
                        int merged = atomsMerged.incrementAndGet();
                        if (merged % ATOMS_TO_UPDATE_PROGRESS == 0)
                            updateBytesRead();
                    }

                    public void close()
                    {
                    }
                };
            }

            public void close()
            {
            }
        };
    }

    private void updateBytesRead()
    {
        long n = 0;
        for (ICompactionScanner scanner : scanners)
            n += scanner.getCurrentPosition();
        bytesRead = n;
    }

    public boolean hasNext()
    {
        return mergedIterator.hasNext();
    }

    public AtomIterator next()
    {
        return mergedIterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        try
        {
            mergedIterator.close();
        }
        finally
        {
            if (metrics != null)
                metrics.finishCompaction(this);
        }
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    private static class PurgingPartitionIterator extends AbstractFilteringIterator
    {
        private final PurgingRow purgingRow;

        private PurgingPartitionIterator(PartitionIterator toPurge, PurgingRow row)
        {
            super(toPurge, row);
            this.purgingRow = row; // Saving this in a instance variable to avoid type casting to PurgingRow everytime
        }

        private static PurgingPartitionIterator create(PartitionIterator toPurge, CompactionController controller)
        {
            PurgingRow row = new PurgingRow(controller);
            return new PurgingPartitionIterator(toPurge, row);
        }

        protected boolean shouldFilter(AtomIterator atoms)
        {
            purgingRow.update(atoms.partitionKey());

            // TODO: we could be able to skip filtering if AtomIterator was giving us some stats
            // (like the smallest local deletion time).
            return true;
        }

        protected boolean includePartitionDeletion(DeletionTime dt)
        {
            return purgingRow.include(dt);
        }

        protected boolean shouldFilterRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            return purgingRow.include(marker.delTime());
        }

        private static class PurgingRow extends FilteringRow
        {
            private final CompactionController controller;
            private long maxPurgeableTimestamp;

            public PurgingRow(CompactionController controller)
            {
                this.controller = controller;
            }

            public void update(DecoratedKey key)
            {
                // tombstones with a localDeletionTime before this can be purged.  This is the minimum timestamp for any sstable
                // containing `key` outside of the set of sstables involved in this compaction.
                this.maxPurgeableTimestamp = controller.maxPurgeableTimestamp(key);
            }

            public boolean include(DeletionTime dt)
            {
                return dt.markedForDeleteAt() >= maxPurgeableTimestamp
                    || dt.localDeletionTime() >= controller.gcBefore;
            }

            protected boolean includeCell(Cell cell)
            {
                return cell.timestamp() >= maxPurgeableTimestamp
                    || cell.localDeletionTime() >= controller.gcBefore;
            }

            protected boolean includeDeletion(ColumnDefinition c, DeletionTime dt)
            {
                return dt.markedForDeleteAt() >= maxPurgeableTimestamp
                    || dt.localDeletionTime() >= controller.gcBefore;
            }
        }
    }
}
