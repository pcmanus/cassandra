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
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.ColumnFilter;
import org.apache.cassandra.db.filters.DataLimits;
import org.apache.cassandra.db.filters.SlicePartitionFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionSliceCommand extends SinglePartitionReadCommand<SlicePartitionFilter>
{
    public SinglePartitionSliceCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       SlicePartitionFilter partitionFilter)
    {
        super(metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    public SinglePartitionSliceCommand copy()
    {
        return new SinglePartitionSliceCommand(metadata(), nowInSec(), columnFilter(), limits(), partitionKey(), partitionFilter());
    }

    public SinglePartitionReadCommand maybeGenerateRetryCommand(DataResolver resolver, Row row)
    {
        // TODO
        throw new UnsupportedOperationException();
        //int maxLiveColumns = resolver.getMaxLiveCount();

        //int count = filter.count;
        //// We generate a retry if at least one node reply with count live columns but after merge we have less
        //// than the total number of column we are interested in (which may be < count on a retry).
        //// So in particular, if no host returned count live columns, we know it's not a short read.
        //if (maxLiveColumns < count)
        //    return null;

        //int liveCountInRow = row == null || row.cf == null ? 0 : filter.getLiveCount(row.cf, timestamp);
        //if (liveCountInRow < getOriginalRequestedCount())
        //{
        //    // We asked t (= count) live columns and got l (=liveCountInRow) ones.
        //    // From that, we can estimate that on this row, for x requested
        //    // columns, only l/t end up live after reconciliation. So for next
        //    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
        //    int retryCount = liveCountInRow == 0 ? count + 1 : ((count * count) / liveCountInRow) + 1;
        //    SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
        //    return new RetriedSliceFromReadCommand(ksName, key, cfName, timestamp, newFilter, getOriginalRequestedCount());
        //}

        //return null;
    }

    protected AtomIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(partitionKey()));

        List<AtomIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        SlicePartitionFilter filter = partitionFilter();

        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey());
            if (partition == null)
                continue;

            AtomIterator iter = filter.getAtomIterator(partition);
            if (copyOnHeap)
                iter = AtomIterators.cloningIterator(iter, HeapAllocator.instance);
            iterators.add(iter);
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
            AtomIterator iter = filter.getSSTableAtomIterator(sstable, partitionKey());
            mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone, iter.partitionLevelDeletion().markedForDeleteAt());
            sstablesIterated++;
            iterators.add(iter);
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
                AtomIterator iter = filter.getSSTableAtomIterator(sstable, partitionKey());
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
            Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones", new Object[] {nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones});

        cfs.metric.updateSSTableIterated(sstablesIterated);

        if (iterators.isEmpty())
            return AtomIterators.emptyIterator(cfs.metadata, partitionKey(), filter.isReversed());

        Tracing.trace("Merging data from memtables and {} sstables", sstablesIterated);
        return AtomIterators.merge(iterators, nowInSec());
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
