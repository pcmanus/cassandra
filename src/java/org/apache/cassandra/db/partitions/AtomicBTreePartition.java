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
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.service.StorageService;
import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

public class AtomicBTreePartition implements Partition
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(CFMetaData.IndexCf,
                                                                                       StorageService.getPartitioner().decorateKey(ByteBuffer.allocate(1))));

    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");

    private static final DeletionInfo LIVE = DeletionInfo.live();
    // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
    // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE, null);

    private final CFMetaData metadata;
    private final DecoratedKey partitionKey;

    private volatile Holder ref;

    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, Holder.class, "ref");

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
    }

    public boolean isEmpty()
    {
        return ref.deletionInfo.isLive() && BTree.isEmpty(ref.tree);
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return ref.deletionInfo.getTopLevelDeletion();
    }

    public boolean hasRows()
    {
        return !BTree.isEmpty(ref.tree);
    }

    public SearchIterator<ClusteringPrefix, Row> searchIterator()
    {
        // TODO: we could optimize comparison for "NativeRow" à la #6755
        return new BTreeSearchIterator<>(ref.tree, metadata.comparator);
    }

    public AtomIterator atomIterator(Slices slices, boolean reversed)
    {
        if (slices.size() == 0)
            return AtomIterators.emptyIterator(metadata, partitionKey, reversed);

        return slices.size() == 1
             ? new SingleSliceIterator(metadata, partitionKey, partitionLevelDeletion(), ref, slices.lowerBound(), slices.upperBound(), !reversed)
             : new SlicesIterator(metadata, partitionKey, partitionLevelDeletion(), ref, slices, !reversed);
    }

    private static class SingleSliceIterator extends AbstractIterator<Atom> implements AtomIterator
    {
        private final CFMetaData metadata;
        private final DecoratedKey partitionKey;
        private final DeletionTime partitionLevelDeletion;
        private final Row staticRow;
        private final boolean forwards;

        private final Iterator<RangeTombstone> tombstoneIter;
        private final Iterator<RowUpdate> rowIter;

        private RangeTombstone nextTombstone;
        private boolean inTombstone;
        private Row nextRow;

        private final ReusableRangeTombstoneMarker marker = new ReusableRangeTombstoneMarker();

        private SingleSliceIterator(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionTime partitionLevelDeletion,
                                    Holder holder,
                                    ClusteringPrefix start,
                                    ClusteringPrefix end,
                                    boolean forwards)
        {
            this.metadata = metadata;
            this.partitionKey = key;
            this.partitionLevelDeletion = partitionLevelDeletion;
            this.staticRow = holder.staticRow;
            this.forwards = forwards;
            this.rowIter = BTree.slice(holder.tree,
                                       metadata.comparator,
                                       start == EmptyClusteringPrefix.BOTTOM ? null : start,
                                       true,
                                       end == EmptyClusteringPrefix.TOP  ? null : end,
                                       true,
                                       forwards);
            this.tombstoneIter = holder.deletionInfo.rangeIterator(start, end);
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public Row staticRow()
        {
            return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
        }

        public Columns columns()
        {
            return metadata.regularColumns();
        }

        public Columns staticColumns()
        {
            return metadata.staticColumns();
        }

        public boolean isReverseOrder()
        {
            return !forwards;
        }

        private void prepareNextTombstone()
        {
            while (nextTombstone == null && tombstoneIter.hasNext())
            {
                inTombstone = false;
                nextTombstone = tombstoneIter.next();
                if (partitionLevelDeletion.supersedes(nextTombstone.data))
                    nextTombstone = null;
            }
        }

        private void prepareNextRow()
        {
            if (nextRow == null && rowIter.hasNext())
                nextRow = rowIter.next();
        }

        private Row filterShadowed(Row row)
        {
            if (partitionLevelDeletion.isLive() && !inTombstone)
                return row;

            long deletionTimestamp = Math.max(partitionLevelDeletion.markedForDeleteAt(),
                                              inTombstone ? nextTombstone.data.markedForDeleteAt() : Long.MIN_VALUE);

            return Rows.withoutShadowed(row, deletionTimestamp);
        }

        protected Atom computeNext()
        {
            while (true)
            {
                prepareNextTombstone();
                prepareNextRow();

                if (nextTombstone == null)
                {
                    if (nextRow == null)
                        return endOfData();

                    Row row = filterShadowed(nextRow);
                    nextRow = null;

                    if (row != null)
                        return row;
                    else
                        continue;
                }

                if (nextRow == null)
                {
                    if (inTombstone)
                    {
                        RangeTombstone rt = nextTombstone;
                        nextTombstone = null;
                        return marker.setTo(rt.max, false, rt.data);
                    }

                    inTombstone = true;
                    return marker.setTo(nextTombstone.min, true, nextTombstone.data);
                }

                if (inTombstone)
                {
                    if (metadata.comparator.compare(nextTombstone.max, nextRow) < 0)
                    {
                        RangeTombstone rt = nextTombstone;
                        nextTombstone = null;
                        return marker.setTo(rt.max, false, rt.data);
                    }
                    else
                    {
                        Row row = filterShadowed(nextRow);
                        nextRow = null;

                        if (row != null)
                            return row;
                        else
                            continue;
                    }
                }

                if (metadata.comparator.compare(nextTombstone.min, nextRow) < 0)
                {
                    inTombstone = true;
                    return marker.setTo(nextTombstone.min, true, nextTombstone.data);
                }
                else
                {
                    Row row = filterShadowed(nextRow);
                    nextRow = null;

                    if (row != null)
                        return row;
                    else
                        continue;
                }
            }
        }

        public void close()
        {
        }
    }

    public static class SlicesIterator extends AbstractIterator<Atom> implements AtomIterator
    {
        private final CFMetaData metadata;
        private final DecoratedKey partitionKey;
        private final DeletionTime partitionLevelDeletion;
        private final Holder holder;
        private final Slices slices;
        private final boolean forwards;

        private int idx;
        private AtomIterator currentSlice;

        private SlicesIterator(CFMetaData metadata,
                               DecoratedKey key,
                               DeletionTime partitionLevelDeletion,
                               Holder holder,
                               Slices slices,
                               boolean forwards)
        {
            this.metadata = metadata;
            this.partitionKey = key;
            this.partitionLevelDeletion = partitionLevelDeletion;
            this.holder = holder;
            this.slices = slices;
            this.forwards = forwards;
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public Row staticRow()
        {
            return holder.staticRow == null ? Rows.EMPTY_STATIC_ROW : holder.staticRow;
        }

        public Columns columns()
        {
            return metadata.regularColumns();
        }

        public Columns staticColumns()
        {
            return metadata.staticColumns();
        }

        public boolean isReverseOrder()
        {
            return !forwards;
        }

        protected Atom computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.size())
                    return endOfData();

                int sliceIdx = forwards ? idx : slices.size() - idx - 1;
                currentSlice = new SingleSliceIterator(metadata,
                                                       partitionKey,
                                                       partitionLevelDeletion,
                                                       holder,
                                                       slices.getStart(sliceIdx),
                                                       slices.getEnd(sliceIdx),
                                                       forwards);
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }

        public void close()
        {
        }
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return the difference in size seen after merging the updates
     */
    public long addAllWithSizeDelta(final PartitionUpdate update, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
    {
        RowUpdater updater = new RowUpdater(this, update.metadata(), allocator, writeOp, indexer);
        DeletionInfo inputDeletionInfoCopy = null;

        boolean monitorOwned = false;
        try
        {
            if (usePessimisticLocking())
            {
                Locks.monitorEnterUnsafe(this);
                monitorOwned = true;
            }
            while (true)
            {
                Holder current = ref;
                updater.ref = current;
                updater.reset();

                DeletionInfo deletionInfo;
                if (update.deletionInfo().mayModify(current.deletionInfo))
                {
                    if (inputDeletionInfoCopy == null)
                        inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);

                    deletionInfo = current.deletionInfo.copy().add(inputDeletionInfoCopy);
                    updater.allocated(deletionInfo.unsharedHeapSize() - current.deletionInfo.unsharedHeapSize());
                }
                else
                {
                    deletionInfo = current.deletionInfo;
                }

                RowUpdate newStatic = update.staticRowUpdate();
                RowUpdate staticRow = newStatic == null
                                    ? current.staticRow
                                    : (current.staticRow == null ? updater.apply(newStatic) : updater.apply(current.staticRow, newStatic));
                Object[] tree = BTree.update(current.tree, update.metadata().comparator.rowUpdateComparator, update, update.rowCount(), true, updater);

                if (tree != null && refUpdater.compareAndSet(this, current, new Holder(tree, deletionInfo, staticRow)))
                {
                    indexer.updateRowLevelIndexes();
                    updater.finish();
                    return updater.dataSize;
                }
                else if (!monitorOwned)
                {
                    boolean shouldLock = usePessimisticLocking();
                    if (!shouldLock)
                    {
                        shouldLock = updateWastedAllocationTracker(updater.heapSize);
                    }
                    if (shouldLock)
                    {
                        Locks.monitorEnterUnsafe(this);
                        monitorOwned = true;
                    }
                }
            }
        }
        finally
        {
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }

    }

    boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes) {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (System.nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }

    private static final class Holder
    {
        final DeletionInfo deletionInfo;
        // the btree of rows
        final Object[] tree;
        final RowUpdate staticRow;

        Holder(Object[] tree, DeletionInfo deletionInfo, RowUpdate staticRow)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info, this.staticRow);
        }
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<RowUpdate>
    {
        final AtomicBTreePartition updating;
        final CFMetaData metadata;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final Updater indexer;
        Holder ref;
        long dataSize;
        long heapSize;
        final MemtableAllocator.DataReclaimer reclaimer;
        List<RowUpdate> inserted; // TODO: replace with walk of aborted BTree

        private RowUpdater(AtomicBTreePartition updating, CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.metadata = metadata;
            this.reclaimer = allocator.reclaimer();
        }

        public RowUpdate apply(RowUpdate insert)
        {
            insertIntoIndexes(insert);
            insert = insert.localCopy(metadata, allocator, writeOp);
            this.dataSize += insert.dataSize();
            this.heapSize += insert.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(insert);
            return insert;
        }

        public RowUpdate apply(RowUpdate existing, RowUpdate update)
        {
            RowUpdate reconciled = existing.mergeTo(update, indexer);
            if (existing != reconciled)
            {
                reconciled = reconciled.localCopy(metadata, allocator, writeOp);
                dataSize += reconciled.dataSize() - existing.dataSize();
                heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
                if (inserted == null)
                    inserted = new ArrayList<>();
                inserted.add(reconciled);
                discard(existing);
            }
            return reconciled;
        }

        private void insertIntoIndexes(Row row)
        {
            ClusteringPrefix clustering = row.clustering();
            for (ColumnData data : row)
            {
                ColumnDefinition c = data.column();
                for (int i = 0; i < data.size(); i++)
                    indexer.insert(clustering, c, data.cell(i));
            }
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
            {
                for (RowUpdate row : inserted)
                    abort(row);
                inserted.clear();
            }
            reclaimer.cancel();
        }

        protected void abort(RowUpdate abort)
        {
            reclaimer.reclaimImmediately(abort);
        }

        protected void discard(RowUpdate discard)
        {
            reclaimer.reclaim(discard);
        }

        public boolean abortEarly()
        {
            return updating.ref != ref;
        }

        public void allocated(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void finish()
        {
            allocator.onHeap().allocate(heapSize, writeOp);
            reclaimer.commit();
        }
    }
}
