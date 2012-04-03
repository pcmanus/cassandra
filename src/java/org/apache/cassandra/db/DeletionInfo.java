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

import org.apache.cassandra.io.ISerializer;

public class DeletionInfo
{
    private static final DeletionInfoSerializer serializer = new DeletionInfoSerializer();

    private final long markedForDeleteAt;
    private final int localDeletionTime;

    public static final DeletionInfo LIVE = new DeletionInfo(Long.MIN_VALUE, Integer.MAX_VALUE);

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        if (localDeletionTime == Integer.MIN_VALUE)
            localDeletionTime = Integer.MAX_VALUE;

        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
    }

    public static ISerializer<DeletionInfo> serializer()
    {
        return serializer;
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return markedForDeleteAt == Long.MIN_VALUE && localDeletionTime == Integer.MAX_VALUE;
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
        return !isLive() && timestamp <= markedForDeleteAt;
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

    private static class DeletionInfoSerializer implements ISerializer<DeletionInfo>
    {
        public void serialize(DeletionInfo info, DataOutput out) throws IOException
        {
            out.writeInt(info.localDeletionTime);
            out.writeLong(info.markedForDeleteAt);
        }

        public DeletionInfo deserialize(DataInput in) throws IOException
        {
            int ldt = in.readInt();
            long mfda = in.readLong();
            if (mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE)
                return LIVE;
            else
                return new DeletionInfo(mfda, ldt);
        }

        public long serializedSize(DeletionInfo info)
        {
            return DBConstants.INT_SIZE + DBConstants.LONG_SIZE;
        }
    }
}
