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

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DiskAwareRunnable;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.*;

public class Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    static final MemtablePool MEMORY_POOL = DatabaseDescriptor.getMemtableAllocatorPool();
    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the last ReplayPosition owned by this Memtable; all ReplayPositions lower are owned by this or an earlier Memtable
    private final AtomicReference<ReplayPosition> lastReplayPosition = new AtomicReference<>();
    // the "first" ReplayPosition owned by this Memtable; this is inaccurate, and only used as a convenience to prevent CLSM flushing wantonly
    private final ReplayPosition minReplayPosition = CommitLog.instance.getContext();

    // We index the memtable by RowPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<RowPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationTime = System.currentTimeMillis();
    private final long creationNano = System.nanoTime();

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final ColumnsCollector columnsCollector = new ColumnsCollector();
    private final StatsCollector statsCollector = new StatsCollector();

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    void setDiscarding(OpOrder.Barrier writeBarrier, ReplayPosition minLastReplayPosition)
    {
        assert this.writeBarrier == null;
        this.lastReplayPosition.set(minLastReplayPosition);
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    public boolean accepts(OpOrder.Group opGroup)
    {
        OpOrder.Barrier barrier = this.writeBarrier;
        return barrier == null || barrier.isAfter(opGroup);
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    public boolean isCleanAfter(ReplayPosition position)
    {
        return isClean() || (position != null && minReplayPosition.compareTo(position) >= 0);
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.getMemtableFlushPeriod();
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * replayPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    void put(PartitionUpdate update, SecondaryIndexManager.Updater indexer, OpOrder.Group opGroup, ReplayPosition replayPosition)
    {
        if (replayPosition != null && writeBarrier != null)
        {
            // if the writeBarrier is set, we want to maintain lastReplayPosition; this is an optimisation to avoid
            // casing it for every write, but still ensure it is correct when writeBarrier.await() completes.
            while (true)
            {
                ReplayPosition last = lastReplayPosition.get();
                if (last.compareTo(replayPosition) >= 0)
                    break;
                if (lastReplayPosition.compareAndSet(last, replayPosition))
                    break;
            }
        }

        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        if (previous == null)
        {
            final DecoratedKey cloneKey = allocator.clone(update.partitionKey(), opGroup);
            AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey, allocator);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = partitions.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (cfs.partitioner.getHeapSizeOf(cloneKey.getToken()) + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
            }
            else
            {
                allocator.reclaimer().reclaimImmediately(cloneKey);
            }
        }

        liveDataSize.addAndGet(previous.addAllWithSizeDelta(update, opGroup, indexer));
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
    }

    // for debugging
    //public String contents()
    //{
    //    StringBuilder builder = new StringBuilder();
    //    builder.append("{");
    //    for (Map.Entry<RowPosition, AtomicBTreeColumns> entry : rows.entrySet())
    //    {
    //        builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
    //    }
    //    builder.append("}");
    //    return builder.toString();
    //}

    public FlushRunnable flushRunnable()
    {
        return new FlushRunnable(lastReplayPosition.get());
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), liveDataSize, currentOperations, 100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    public PartitionIterator makePartitionIterator(final DataRange dataRange)
    {
        AbstractBounds<RowPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum(cfs.partitioner);
        boolean stopIsMin = keyRange.right.isMinimum(cfs.partitioner);

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        Map<RowPosition, AtomicBTreePartition> subMap = null;
        if (startIsMin)
            subMap = stopIsMin ? partitions : partitions.headMap(keyRange.right, includeStop);
        else
            subMap = stopIsMin
                   ? partitions.tailMap(keyRange.left, includeStart)
                   : partitions.subMap(keyRange.left, includeStart, keyRange.right, includeStop);

        final Iterator<Map.Entry<RowPosition, AtomicBTreePartition>> iter = subMap.entrySet().iterator();

        return new PartitionIterator()
        {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public AtomIterator next()
            {
                Map.Entry<RowPosition, AtomicBTreePartition> entry = iter.next();
                // Actual stored key should be true DecoratedKey
                assert entry.getKey() instanceof DecoratedKey;
                DecoratedKey key = (DecoratedKey)entry.getKey();
                PartitionFilter filter = dataRange.partitionFilter(cfs.getComparator(), key);
                return filter.getAtomIterator(entry.getValue());
            }

            public void remove()
            {
                // It wouldn't be too hard to implement but I don't think we ever use it
                throw new UnsupportedOperationException();
            }

            public void close()
            {
            }
        };
    }

    public Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    public long creationTime()
    {
        return creationTime;
    }

    public ReplayPosition getLastReplayPosition()
    {
        return lastReplayPosition.get();
    }

    class FlushRunnable extends DiskAwareRunnable
    {
        private final ReplayPosition context;
        private final long estimatedSize;

        private final boolean isBatchLogTable;

        FlushRunnable(ReplayPosition context)
        {
            this.context = context;

            long keySize = 0;
            for (RowPosition key : partitions.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keySize += ((DecoratedKey)key).getKey().remaining();
            }
            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + liveDataSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead

            this.isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHLOG_CF) && cfs.keyspace.getName().equals(Keyspace.SYSTEM_KS);
        }

        public long getExpectedWriteSize()
        {
            return estimatedSize;
        }

        protected void runWith(File sstableDirectory) throws Exception
        {
            assert sstableDirectory != null : "Flush task is not bound to any disk";

            SSTableReader sstable = writeSortedContents(context, sstableDirectory);
            cfs.replaceFlushed(Memtable.this, sstable);
        }

        protected Directories getDirectories()
        {
            return cfs.directories;
        }

        private SSTableReader writeSortedContents(ReplayPosition context, File sstableDirectory)
        {
            logger.info("Writing {}", Memtable.this.toString());

            SSTableReader ssTable;
            // errors when creating the writer that may leave empty temp files.
            SSTableWriter writer = createFlushWriter(cfs.getTempSSTablePath(sstableDirectory), columnsCollector.get(), statsCollector.get());
            try
            {
                // (we can't clear out the map as-we-go to free up memory,
                //  since the memtable is being used for queries in the "pending flush" category)
                for (AtomicBTreePartition partition : partitions.values())
                {
                    // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                    // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                    // we don't need to preserve tombstones for repair. So if both operation are in this
                    // memtable (which will almost always be the case if there is no ongoing failure), we can
                    // just skip the entry (CASSANDRA-4667).
                    if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                        continue;

                    if (!partition.isEmpty())
                        writer.append(partition.atomIterator(partition.columns(), Slices.ALL, false));
                }

                if (writer.getFilePointer() > 0)
                {
                    writer.isolateReferences();

                    // temp sstables should contain non-repaired data.
                    ssTable = writer.closeAndOpenReader();
                    logger.info(String.format("Completed flushing %s (%d bytes) for commitlog position %s",
                                              ssTable.getFilename(), new File(ssTable.getFilename()).length(), context));
                }
                else
                {
                    writer.abort();
                    ssTable = null;
                    logger.info("Completed flushing; nothing needed to be retained.  Commitlog position was {}",
                                context);
                }

                return ssTable;
            }
            catch (Throwable e)
            {
                writer.abort();
                throw Throwables.propagate(e);
            }
        }

        public SSTableWriter createFlushWriter(String filename,
                                               PartitionColumns columns,
                                               AtomStats stats)
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator).replayPosition(context);
            return new SSTableWriter(filename,
                                     partitions.size(),
                                     ActiveRepairService.UNREPAIRED_SSTABLE,
                                     cfs.metadata,
                                     cfs.partitioner,
                                     sstableMetadataCollector,
                                     new SerializationHeader(cfs.metadata, columns, stats));
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        final OpOrder.Group group = new OpOrder().start();
        int rowOverhead;
        MemtableAllocator allocator = MEMORY_POOL.newAllocator();
        ConcurrentNavigableMap<RowPosition, Object> partitions = new ConcurrentSkipListMap<>();
        final Object val = new Object();
        for (int i = 0 ; i < count ; i++)
            partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken((long) i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
        double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
        rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
        rowOverhead -= ObjectSizes.measureDeep(new LongToken((long) 0));
        rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
        allocator.setDiscarding();
        allocator.setDiscarded();
        return rowOverhead;
    }

    private static class ColumnsCollector
    {
        // TODO: we could probably do more efficient, but I'm being lazy
        private final ConcurrentSkipListSet<ColumnDefinition> columns = new ConcurrentSkipListSet<>();

        public void update(PartitionColumns columns)
        {
            for (ColumnDefinition s : columns.statics)
                this.columns.add(s);
            for (ColumnDefinition r : columns.regulars)
                this.columns.add(r);
        }

        public PartitionColumns get()
        {
            PartitionColumns.builder().addAll(columns).build();
        }
    }

    private static class StatsCollector
    {
        private final AtomicReference<AtomStats> stats = new AtomicReference<>(AtomStats.NO_STATS);

        public void update(AtomStats newStats)
        {
            while (true)
            {
                AtomStats current = stats.get();
                AtomStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public AtomStats get()
        {
            return stats.get();
        }
    }
}
