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

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.SlicePartitionFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.ColumnFamilyMetrics.Sampler;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionSliceCommand extends SinglePartitionReadCommand<SlicePartitionFilter>
{
    public SinglePartitionSliceCommand(boolean isDigest,
                                       boolean isForThrift,
                                       CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       SlicePartitionFilter partitionFilter)
    {
        super(isDigest, isForThrift, metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    public SinglePartitionSliceCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       SlicePartitionFilter partitionFilter)
    {
        this(false, false, metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or column filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or column filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slices slices)
    {
        SlicePartitionFilter filter = new SlicePartitionFilter(metadata.partitionColumns(), slices, false);
        return new SinglePartitionSliceCommand(metadata, nowInSec, ColumnFilter.NONE, DataLimits.NONE, key, filter);
    }

    public SinglePartitionSliceCommand copy()
    {
        return new SinglePartitionSliceCommand(isDigestQuery(), isForThrift(), metadata(), nowInSec(), columnFilter(), limits(), partitionKey(), partitionFilter());
    }

    protected UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(partitionKey()));

        List<UnfilteredRowIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        SlicePartitionFilter filter = partitionFilter();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                Partition partition = memtable.getPartition(partitionKey());
                if (partition == null)
                    continue;

                UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(partition, nowInSec());
                if (copyOnHeap)
                    iter = UnfilteredRowIterators.cloningIterator(iter, HeapAllocator.instance);
                iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter) : iter);
            }

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 > maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in maxTimestamp order allow to do our mostRecentPartitionTombstone elimination
             * in one pass, and minimize the number of sstables for which we read a partition tombstone.
             */
            int sstablesIterated = 0;
            Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
            List<SSTableReader> skippedSSTables = null;
            long mostRecentPartitionTombstone = Long.MIN_VALUE;
            long minTimestamp = Long.MAX_VALUE;
            int nonIntersectingSSTables = 0;

            for (SSTableReader sstable : view.sstables)
            {
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                    break;

                if (!filter.shouldInclude(sstable))
                {
                    nonIntersectingSSTables++;
                    // sstable contains no tombstone if maxLocalDeletionTime == Integer.MAX_VALUE, so we can safely skip those entirely
                    if (sstable.getSSTableMetadata().maxLocalDeletionTime != Integer.MAX_VALUE)
                    {
                        if (skippedSSTables == null)
                            skippedSSTables = new ArrayList<>();
                        skippedSSTables.add(sstable);
                    }
                    continue;
                }

                sstable.incrementReadCount();
                UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), filter.queriedColumns(), filter.isReversed(), nowInSec(), isForThrift()));
                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone, iter.partitionLevelDeletion().markedForDeleteAt());
                sstablesIterated++;
                iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter) : iter);
            }

            int includedDueToTombstones = 0;
            // Check for partition tombstones in the skipped sstables
            if (skippedSSTables != null)
            {
                for (SSTableReader sstable : skippedSSTables)
                {
                    if (sstable.getMaxTimestamp() <= minTimestamp)
                        continue;

                    sstable.incrementReadCount();
                    UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), filter.queriedColumns(), filter.isReversed(), nowInSec(), isForThrift()));
                    if (iter.partitionLevelDeletion().markedForDeleteAt() > minTimestamp)
                    {
                        includedDueToTombstones++;
                        iterators.add(iter);
                        sstablesIterated++;
                    }
                    else
                    {
                        FileUtils.closeQuietly(iter);
                    }
                }
            }
            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                              nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            cfs.metric.updateSSTableIterated(sstablesIterated);

            if (iterators.isEmpty())
                return UnfilteredRowIterators.emptyIterator(cfs.metadata, partitionKey(), filter.isReversed(), nowInSec());

            Tracing.trace("Merging data from memtables and {} sstables", sstablesIterated);

            UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators);
            if (!merged.isEmpty())
            {
                DecoratedKey key = merged.partitionKey();
                cfs.metric.samplers.get(Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
            }

            return merged;
        }
        catch (RuntimeException | Error e)
        {
            FileUtils.closeQuietly(iterators);
            throw e;
        }
    }

// From SliceFromReadCommand
//class SliceFromReadCommandSerializer implements IVersionedSerializer<ReadCommand>
//{
//    public void serialize(ReadCommand rm, DataOutputPlus out, int version) throws IOException
//    {
//        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
//        out.writeBoolean(realRM.isDigestQuery());
//        out.writeUTF(realRM.ksName);
//        ByteBufferUtil.writeWithShortLength(realRM.key, out);
//        out.writeUTF(realRM.cfName);
//        out.writeLong(realRM.timestamp);
//        CFMetaData metadata = Schema.instance.getCFMetaData(realRM.ksName, realRM.cfName);
//        metadata.comparator.sliceQueryFilterSerializer().serialize(realRM.filter, out, version);
//    }
//
//    public ReadCommand deserialize(DataInput in, int version) throws IOException
//    {
//        boolean isDigest = in.readBoolean();
//        String keyspaceName = in.readUTF();
//        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
//        String cfName = in.readUTF();
//        long timestamp = in.readLong();
//        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
//        SliceQueryFilter filter = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);
//        ReadCommand command = new SliceFromReadCommand(keyspaceName, key, cfName, timestamp, filter);
//        command.setDigestQuery(isDigest);
//        return command;
//    }
//
//    public long serializedSize(ReadCommand cmd, int version)
//    {
//        TypeSizes sizes = TypeSizes.NATIVE;
//        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
//        int keySize = command.key.remaining();
//
//        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);
//
//        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
//        size += sizes.sizeof(command.ksName);
//        size += sizes.sizeof((short) keySize) + keySize;
//        size += sizes.sizeof(command.cfName);
//        size += sizes.sizeof(cmd.timestamp);
//        size += metadata.comparator.sliceQueryFilterSerializer().serializedSize(command.filter, version);
//
//        return size;
//    }
//}
}
