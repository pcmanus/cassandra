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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.io.IVersionedSerializer;

public class DeletionInfo
{
    private static final DeletionInfoSerializer serializer = new DeletionInfoSerializer();

    // We don't have way to represent the full interval of keys (Interval don't support the minimum token as the right bound),
    // so we keep the topLevel deletion info separatly. This also slightly optimize the case of full row deletion which is rather common.
    private final DeletionTime topLevel;
    private final IntervalTree<ByteBuffer, DeletionTime> ranges;

    public static final DeletionInfo LIVE = new DeletionInfo(LIVE_DELETION_TIME, IntervalTree.emptyTree());
    private static final DeletionTime LIVE_DELETION_TIME = new DeletionTime(Long.MIN_VALUE, Integer.MAX_VALUE);

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(new DeletionTime(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime), IntervalTree.emptyTree());
    }

    public DeletionInfo(RowPosition left, RowPosition right, long markedForDeleteAt, int localDeletionTime)
    {
        this(LIVE_DELETION_TIME, new IntervalTree<RowPosition, DeletionTime>(Interval.create(left, right, new DeletionTime(markedForDeleteAt, localDeletionTime))));
    }

    private DeletionInfo(DeletionTime topLevel, IntervalTree<RowPosition, DeletionTime> ranges)
    {
        this.topLevel = topLevel;
        this.ranges = ranges;
    }

    public static IVersionedSerializer<DeletionInfo> serializer()
    {
        return serializer;
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return topLevel.markedForDeleteAt == Long.MIN_VALUE
            && topLevellocalDeletionTime == Integer.MAX_VALUE
            && ranges.isEmpty();
    }

    /**
     * Return whether a given column is deleted by the container having this
     * deletion info.
     *
     * @param col the column to check.
     * @return true if the column is deleted, false otherwise
     */
    public boolean isDeleted(IColumn column)
    {
        return isDeleted(column.name(), column.mostRecentLiveChangeAt());
    }

    public boolean isDeleted(ByteBuffer name, long timestamp)
    {
        if (isLive())
            return false;
        if (timestamp <= topLevel.markedForDeleteAt)
            return true;

        List<Interval<RowPosition, DeletionTime>> tombstones = ranges.search();
    }

    /**
     * Return a new DeletionInfo correspond to purging every tombstones that
     * are older than {@code gcbefore}.
     *
     * @param gcBefore timestamp (in seconds) before which tombstones should
     * be purged
     * @return a new DeletionInfo with the purged info remove. Should return
     * DeletionInfo.LIVE if no tombstones remain.
     */
    public DeletionInfo purge(int gcBefore)
    {
        return localDeletionTime < gcBefore ? LIVE : this;
    }

    /**
     * Returns a new DeletionInfo containing of this plus the provided {@code
     * newInfo}.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        return markedForDeleteAt < newInfo.markedForDeleteAt ? newInfo : this;
    }

    // Only used by SuperColumn currently and should stay that way
    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    // Only used by SuperColumn currently and should stay that way
    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }

    @Override
    public String toString()
    {
        return String.format("{deletedAt=%d, localDeletion=%d}", markedForDeleteAt, localDeletionTime);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        // handles nulls properly
        return markedForDeleteAt == that.markedForDeleteAt && localDeletionTime == that.localDeletionTime;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(markedForDeleteAt, localDeletionTime);
    }

    private static class DeletionTime
    {
        private final long markedForDeleteAt;
        private final int localDeletionTime;

        DeletionTime(long markedForDeleteAt, int localDeletionTime)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
        }
    }

    private static class DeletionInfoSerializer implements IVersionedSerializer<DeletionInfo>
    {
        public void serialize(DeletionInfo info, DataOutput out, int version) throws IOException
        {
            out.writeInt(info.localDeletionTime);
            out.writeLong(info.markedForDeleteAt);
        }

        public DeletionInfo deserialize(DataInput in, int version) throws IOException
        {
            int ldt = in.readInt();
            long mfda = in.readLong();
            if (mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE)
                return LIVE;
            else
                return new DeletionInfo(mfda, ldt);
        }

        public long serializedSize(DeletionInfo info, int version)
        {
            return DBConstants.INT_SIZE + DBConstants.LONG_SIZE;
        }
    }
}
