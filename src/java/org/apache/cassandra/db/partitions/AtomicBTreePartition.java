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
import org.apache.cassandra.utils.FBUtilities;
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
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE, Rows.EMPTY_STATIC_ROW);

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

    public PartitionColumns columns()
    {
        // We don't really know which columns will be part of the update, so assume it's all of them
        return metadata.partitionColumns();
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

    public AtomIterator atomIterator(PartitionColumns columns, Slices slices, boolean reversed)
    {
        if (slices.size() == 0)
            return AtomIterators.emptyIterator(metadata, partitionKey, reversed);

        return slices.size() == 1
             ? new SingleSliceIterator(metadata, partitionKey, partitionLevelDeletion(), ref, columns, slices.lowerBound(), slices.upperBound(), reversed)
             : new SlicesIterator(metadata, partitionKey, partitionLevelDeletion(), ref, columns, slices, reversed);
    }

    private static class SingleSliceIterator extends AbstractAtomIterator
    {
        private final Iterator<RangeTombstone> tombstoneIter;
        private final Iterator<Row> rowIter;

        private final FilteringRow filter;

        private RangeTombstone nextTombstone;
        private boolean inTombstone;
        private Row nextRow;

        private final ReusableRangeTombstoneMarker marker = new ReusableRangeTombstoneMarker();

        private SingleSliceIterator(CFMetaData metadata,
                                    DecoratedKey key,
                                    final DeletionTime partitionLevelDeletion,
                                    Holder holder,
                                    PartitionColumns columns,
                                    ClusteringPrefix start,
                                    ClusteringPrefix end,
                                    boolean isReversed)
        {
            super(metadata, key, partitionLevelDeletion, columns, columns.statics.isEmpty() ? null : holder.staticRow, isReversed);
            this.rowIter = BTree.slice(holder.tree,
                                       metadata.comparator,
                                       start == EmptyClusteringPrefix.BOTTOM ? null : start,
                                       true,
                                       end == EmptyClusteringPrefix.TOP  ? null : end,
                                       true,
                                       !isReversed);
            this.tombstoneIter = holder.deletionInfo.rangeIterator(start, end);

            this.filter = new FilteringRow()
            {
                private long deletionTimestamp;

                @Override
                public FilteringRow setTo(Row row)
                {
                    long rangeDeletion = SingleSliceIterator.this.inTombstone
                                       ? SingleSliceIterator.this.nextTombstone.data.markedForDeleteAt()
                                       : Long.MIN_VALUE;
                    this.deletionTimestamp = Math.max(partitionLevelDeletion.markedForDeleteAt(), rangeDeletion);
                    super.setTo(row);
                    return this;
                }

                @Override
                protected boolean includeTimestamp(long timestamp)
                {
                    return timestamp > deletionTimestamp;
                }

                @Override
                protected boolean include(ColumnDefinition column)
                {
                    return columns().contains(column);
                }

                protected boolean includeCell(Cell cell)
                {
                    return cell.timestamp() > deletionTimestamp;
                }

                protected boolean includeDeletion(ColumnDefinition c, DeletionTime dt)
                {
                    return dt.markedForDeleteAt() > deletionTimestamp;
                }
            };
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
            while (nextRow == null && rowIter.hasNext())
            {
                nextRow = filter.setTo(rowIter.next());
                if (nextRow.isEmpty())
                    nextRow = null;
            }
        }

        protected Atom computeNext()
        {
            prepareNextTombstone();
            prepareNextRow();

            if (nextTombstone == null)
                return nextRow == null ? endOfData() : nextRow;

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
                    return nextRow;
                }
            }

            if (metadata.comparator.compare(nextTombstone.min, nextRow) < 0)
            {
                inTombstone = true;
                return marker.setTo(nextTombstone.min, true, nextTombstone.data);
            }
            else
            {
                return nextRow;
            }
        }
    }

    public static class SlicesIterator extends AbstractAtomIterator
    {
        private final Holder holder;
        private final Slices slices;

        private int idx;
        private AtomIterator currentSlice;

        private SlicesIterator(CFMetaData metadata,
                               DecoratedKey key,
                               DeletionTime partitionLevelDeletion,
                               Holder holder,
                               PartitionColumns columns,
                               Slices slices,
                               boolean isReversed)
        {
            super(metadata, key, partitionLevelDeletion, columns, makeStatic(columns, holder, partitionLevelDeletion), isReversed);
            this.holder = holder;
            this.slices = slices;
        }

        private static Row makeStatic(final PartitionColumns columns, Holder holder, DeletionTime deletion)
        {
            if (columns.statics.isEmpty() || holder.staticRow == null)
                return Rows.EMPTY_STATIC_ROW;

            final long deletionTimestamp = deletion.markedForDeleteAt();

            return new FilteringRow()
            {
                @Override
                protected boolean includeTimestamp(long timestamp)
                {
                    return timestamp > deletionTimestamp;
                }

                @Override
                protected boolean include(ColumnDefinition def)
                {
                    return columns.statics.contains(def);
                }

                protected boolean includeCell(Cell cell)
                {
                    return cell.timestamp() > deletionTimestamp;
                }

                protected boolean includeDeletion(ColumnDefinition c, DeletionTime dt)
                {
                    return dt.markedForDeleteAt() > deletionTimestamp;
                }

            }.setTo(holder.staticRow);
        }

        protected Atom computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.size())
                    return endOfData();

                int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                currentSlice = new SingleSliceIterator(metadata,
                                                       partitionKey,
                                                       partitionLevelDeletion,
                                                       holder,
                                                       columns,
                                                       slices.getStart(sliceIdx),
                                                       slices.getEnd(sliceIdx),
                                                       isReverseOrder);
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
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

                Row newStatic = update.staticRow();
                Row staticRow = newStatic == Rows.EMPTY_STATIC_ROW
                              ? current.staticRow
                              : (current.staticRow == Rows.EMPTY_STATIC_ROW ? updater.apply(newStatic) : updater.apply(current.staticRow, newStatic));
                Object[] tree = BTree.update(current.tree, update.metadata().comparator.rowComparator, update, update.rowCount(), true, updater);

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
    private boolean updateWastedAllocationTracker(long wastedBytes)
    {
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
        final Row staticRow;

        Holder(Object[] tree, DeletionInfo deletionInfo, Row staticRow)
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
    private static final class RowUpdater implements UpdateFunction<Row>
    {
        final AtomicBTreePartition updating;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final Updater indexer;
        final int nowInSec;
        Holder ref;
        long dataSize;
        long heapSize;
        final MemtableAllocator.DataReclaimer reclaimer;
        final MemtableAllocator.RowAllocator rowAllocator;
        List<MemtableRow> inserted; // TODO: replace with walk of aborted BTree

        private RowUpdater(AtomicBTreePartition updating, CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
        {

            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.reclaimer = allocator.reclaimer();
            this.rowAllocator = allocator.newRowAllocator(metadata, writeOp);
        }

        public Row apply(Row insert)
        {
            rowAllocator.allocateNewRow(insert.columns());
            Rows.copy(insert, rowAllocator);
            MemtableRow row = rowAllocator.allocatedRow();

            insertIntoIndexes(row);

            this.dataSize += row.dataSize();
            this.heapSize += row.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(row);
            return row;
        }

        public Row apply(Row existing, Row update)
        {
            Columns mergedColumns = existing.columns().mergeTo(update.columns());
            rowAllocator.allocateNewRow(mergedColumns);

            Rows.merge(existing, update, rowAllocator, nowInSec, indexer);

            MemtableRow reconciled = rowAllocator.allocatedRow();

            assert existing instanceof MemtableRow;
            MemtableRow replaced = (MemtableRow)existing;

            dataSize += reconciled.dataSize() - replaced.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - replaced.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(reconciled);
            discard(replaced);

            return reconciled;
        }

        private void insertIntoIndexes(Row row)
        {
            ClusteringPrefix clustering = row.clustering();
            for (Cell cell : row)
                indexer.insert(clustering, cell);
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
            {
                for (MemtableRow row : inserted)
                    abort(row);
                inserted.clear();
            }
            reclaimer.cancel();
        }

        protected void abort(MemtableRow abort)
        {
            reclaimer.reclaimImmediately(abort);
        }

        protected void discard(MemtableRow discard)
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
