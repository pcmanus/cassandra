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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.compaction.GCParams;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<UnfilteredRowIterator>
{
    private final boolean isForThrift;
    private final DeletionPurger purger;
    private final GCParams gcParams;
    private boolean isReverseOrder;

    public PurgeFunction(boolean isForThrift, GCParams gcParams, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones)
    {
        this.isForThrift = isForThrift;
        this.gcParams = gcParams;

        this.purger = (timestamp, purgingReferenceTime, ttl) ->
        {
            int expiringTime = purgingReferenceTime + ttl;
            int minPurgingTime = purgingReferenceTime + gcParams.gcGraceSeconds();
            // We can't purge before minPurgingTime, but we certainly can't purge before the data is actually expired
            int purgingTime = Math.max(expiringTime, minPurgingTime);
            // There is 3 conditions to met to be allowed to purge the tombstone having the provided data:
            // 1) if the option to only purge tombstone in repaired sstables is set, we only purge if the
            //    purging time is smaller than the smallest purging time in unrepaired sstable (in other worlds, if
            //    we can guarantee the tombstone is from a repaired sstable, see #6434).
            // 2) the tombstone must be purgeable in the first place, i.e. his purging time (which includes GCGrace)
            //    should have pass.
            // 3) the timestamp must be lower than biggest timestamp we're allowed to purge (this is used in practice to
            //    ensure tombstone doesn't shadow something older in a sstable that is not compacted to avoid ressurecting
            //    data. See #8914 and CompactionIterator.Purger#getMaxPurgeableTimestamp.
            return !(onlyPurgeRepairedTombstones && purgingReferenceTime >= oldestUnrepairedTombstone)
                && purgingTime < gcParams.nowInSeconds()
                && timestamp < getMaxPurgeableTimestamp();
        };
    }

    protected abstract long getMaxPurgeableTimestamp();

    // Called at the beginning of each new partition
    protected void onNewPartition(DecoratedKey partitionKey)
    {
    }

    // Called for each partition that had only purged infos and are empty post-purge.
    protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey)
    {
    }

    // Called for every unfiltered. Meant for CompactionIterator to update progress
    protected void updateProgress()
    {
    }

    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        onNewPartition(partition.partitionKey());

        isReverseOrder = partition.isReverseOrder();
        UnfilteredRowIterator purged = Transformation.apply(partition, this);
        if (!isForThrift && purged.isEmpty())
        {
            onEmptyPartitionPostPurge(purged.partitionKey());
            purged.close();
            return null;
        }

        return purged;
    }

    public DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    public Row applyToStatic(Row row)
    {
        updateProgress();
        return row.purge(purger, gcParams.nowInSeconds());
    }

    public Row applyToRow(Row row)
    {
        updateProgress();
        return row.purge(purger, gcParams.nowInSeconds());
    }

    public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        updateProgress();
        boolean reversed = isReverseOrder;
        if (marker.isBoundary())
        {
            // We can only skip the whole marker if both deletion time are purgeable.
            // If only one of them is, filterTombstoneMarker will deal with it.
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
            boolean shouldPurgeClose = purger.shouldPurge(boundary.closeDeletionTime(reversed));
            boolean shouldPurgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));

            if (shouldPurgeClose)
            {
                if (shouldPurgeOpen)
                    return null;

                return boundary.createCorrespondingOpenMarker(reversed);
            }

            return shouldPurgeOpen
                   ? boundary.createCorrespondingCloseMarker(reversed)
                   : marker;
        }
        else
        {
            return purger.shouldPurge(((RangeTombstoneBoundMarker)marker).deletionTime()) ? null : marker;
        }
    }
}
