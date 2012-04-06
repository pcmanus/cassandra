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
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class DeletionInfo
{
    private static final Serializer serializer = new Serializer();

    // We don't have way to represent the full interval of keys (Interval don't support the minimum token as the right bound),
    // so we keep the topLevel deletion info separatly. This also slightly optimize the case of full row deletion which is rather common.
    private final DeletionTime topLevel;
    private final IntervalTree<ByteBuffer, DeletionTime> ranges;

    private static final DeletionTime LIVE_DELETION_TIME = new DeletionTime(Long.MIN_VALUE, Integer.MAX_VALUE);
    public static final DeletionInfo LIVE = new DeletionInfo(LIVE_DELETION_TIME, IntervalTree.<ByteBuffer, DeletionTime>emptyTree());

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(new DeletionTime(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime),
             IntervalTree.<ByteBuffer, DeletionTime>emptyTree());
    }

    public DeletionInfo(ByteBuffer start, ByteBuffer end, Comparator<ByteBuffer> comparator, long markedForDeleteAt, int localDeletionTime)
    {
        this(LIVE_DELETION_TIME,
             IntervalTree.build(Collections.singletonList(Interval.create(start, end, new DeletionTime(markedForDeleteAt, localDeletionTime))), comparator));
    }

    private DeletionInfo(DeletionTime topLevel, IntervalTree<ByteBuffer, DeletionTime> ranges)
    {
        this.topLevel = topLevel;
        this.ranges = ranges;
    }

    public static Serializer serializer()
    {
        return serializer;
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return topLevel.markedForDeleteAt == Long.MIN_VALUE
            && topLevel.localDeletionTime == Integer.MAX_VALUE
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

        for (DeletionTime d : ranges.search(name))
        {
            if (timestamp <= d.markedForDeleteAt)
                return true;
        }
        return false;
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
        if (ranges.isEmpty())
        {
            return topLevel.localDeletionTime < gcBefore ? LIVE : this;
        }
        else
        {
            // We rebuild a new intervalTree that contains only non expired range tombstones
            List<Interval<ByteBuffer, DeletionTime>> nonExpired = new ArrayList<Interval<ByteBuffer, DeletionTime>>();
            for (Interval<ByteBuffer, DeletionTime> range : ranges)
            {
                if (range.data.localDeletionTime >= gcBefore)
                    nonExpired.add(range);
            }
            IntervalTree<ByteBuffer, DeletionTime> newRanges = nonExpired.size() == ranges.intervalCount()
                                                             ? ranges
                                                             : IntervalTree.build(nonExpired, ranges.comparator());
            return topLevel.localDeletionTime < gcBefore
                 ? new DeletionInfo(LIVE_DELETION_TIME, newRanges)
                 : new DeletionInfo(topLevel, newRanges);
        }
    }

    /**
     * Returns a new DeletionInfo containing of this plus the provided {@code
     * newInfo}.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        if (ranges.isEmpty())
        {
            return topLevel.markedForDeleteAt < newInfo.topLevel.markedForDeleteAt
                 ? newInfo
                 : newInfo.ranges.isEmpty() ? this : new DeletionInfo(topLevel, newInfo.ranges);
        }
        else
        {
            if (newInfo.ranges.isEmpty())
            {
                return topLevel.markedForDeleteAt < newInfo.topLevel.markedForDeleteAt
                     ? new DeletionInfo(newInfo.topLevel, ranges)
                     : this;
            }
            else
            {
                // Need to merge both ranges
                Set<Interval<ByteBuffer, DeletionTime>> merged = new HashSet<Interval<ByteBuffer, DeletionTime>>();
                Iterables.addAll(merged, Iterables.concat(ranges, newInfo.ranges));
                return new DeletionInfo(topLevel.markedForDeleteAt < newInfo.topLevel.markedForDeleteAt ? newInfo.topLevel : topLevel,
                                        IntervalTree.build(merged));
            }
        }
    }

    /**
     * The maximum timestamp mentioned by this DeletionInfo.
     */
    public long maxTimestamp()
    {
        long maxTimestamp = topLevel.markedForDeleteAt;
        for (Interval<ByteBuffer, DeletionTime> i : ranges)
        {
            maxTimestamp = Math.max(maxTimestamp, i.data.markedForDeleteAt);
        }
        return maxTimestamp;
    }

    // Only used by SuperColumn currently and should stay that way
    public long getTopLevelMarkedForDeleteAt()
    {
        return topLevel.markedForDeleteAt;
    }

    // Only used by SuperColumn currently and should stay that way
    public int getTopLevelLocalDeletionTime()
    {
        return topLevel.localDeletionTime;
    }

    @Override
    public String toString()
    {
        if (ranges.isEmpty())
            return String.format("{%s}", topLevel);
        else
            return String.format("{%s, ranges=%s}", topLevel, rangesAsString());
    }

    private String rangesAsString()
    {
        assert !ranges.isEmpty();
        StringBuilder sb = new StringBuilder();
        AbstractType at = (AbstractType)ranges.comparator();
        for (Interval<ByteBuffer, DeletionTime> i : ranges)
        {
            sb.append("[");
            sb.append(at.getString(i.min)).append("-");
            sb.append(at.getString(i.max)).append(", ");
            sb.append(i.data);
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        return topLevel.equals(that.topLevel) && ranges.equals(that.ranges);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(topLevel, ranges);
    }

    private static class DeletionTime
    {
        private final long markedForDeleteAt;
        private final int localDeletionTime;

        static final DeletionTimeSerializer serializer = new DeletionTimeSerializer();

        DeletionTime(long markedForDeleteAt, int localDeletionTime)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
        }

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof DeletionTime))
                return false;
            DeletionTime that = (DeletionTime)o;
            return markedForDeleteAt == that.markedForDeleteAt && localDeletionTime == that.localDeletionTime;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(markedForDeleteAt, localDeletionTime);
        }

        @Override
        public String toString()
        {
            return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt, localDeletionTime);
        }

        private static class DeletionTimeSerializer implements ISerializer<DeletionTime>
        {
            public void serialize(DeletionTime delTime, DataOutput out) throws IOException
            {
                out.writeInt(delTime.localDeletionTime);
                out.writeLong(delTime.markedForDeleteAt);
            }

            public DeletionTime deserialize(DataInput in) throws IOException
            {
                int ldt = in.readInt();
                long mfda = in.readLong();
                if (mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE)
                    return DeletionInfo.LIVE_DELETION_TIME;
                else
                    return new DeletionTime(mfda, ldt);
            }

            public long serializedSize(DeletionTime delTime, TypeSizes typeSizes)
            {
                return typeSizes.sizeof(delTime.markedForDeleteAt) + typeSizes.sizeof(delTime.localDeletionTime);
            }
        }
    }

    public static class Serializer implements IVersionedSerializer<DeletionInfo>, ISSTableSerializer<DeletionInfo>
    {
        private final static ISerializer<ByteBuffer> bbSerializer = new ISerializer<ByteBuffer>()
        {
            public void serialize(ByteBuffer bb, DataOutput dos) throws IOException
            {
                ByteBufferUtil.writeWithShortLength(bb, dos);
            }

            public ByteBuffer deserialize(DataInput dis) throws IOException
            {
                return ByteBufferUtil.readWithShortLength(dis);
            }

            public long serializedSize(ByteBuffer bb, TypeSizes typeSizes)
            {
                int bbSize = bb.remaining();
                return typeSizes.sizeof((short)bbSize) + bbSize;
            }
        };

        private final static IntervalTree.Serializer<ByteBuffer, DeletionTime> itSerializer = IntervalTree.serializer(bbSerializer, DeletionTime.serializer);

        public void serialize(DeletionInfo info, DataOutput out, int version) throws IOException
        {
            DeletionTime.serializer.serialize(info.topLevel, out);
            // Pre-1.2 version don't know about range tombstones and thus users should upgrade all
            // nodes before using them. If they didn't, better fail early that propagating bad info
            if (version < MessagingService.VERSION_12)
            {
                if (!info.ranges.isEmpty())
                    throw new RuntimeException("Cannot send range tombstone to pre-1.2 node. You should upgrade all node to Cassandra 1.2+ before using range tombstone.");
                // Otherwise we're done
            }
            else
            {
                itSerializer.serialize(info.ranges, out, version);
            }
        }

        /*
         * Range tombstones internally depend on the column family serializer, but it is not serialized.
         * Thus deserialize(DataInput, int, Comparator<ByteBuffer>) should be used instead of this method.
         */
        public DeletionInfo deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public DeletionInfo deserialize(DataInput in, int version, Comparator<ByteBuffer> comparator) throws IOException
        {
            DeletionTime topLevel = DeletionTime.serializer.deserialize(in);
            if (version < MessagingService.VERSION_12)
                return new DeletionInfo(topLevel, IntervalTree.<ByteBuffer, DeletionTime>emptyTree());

            IntervalTree<ByteBuffer, DeletionTime> ranges = itSerializer.deserialize(in, version, comparator);
            return new DeletionInfo(topLevel, ranges);
        }

        public long serializedSize(DeletionInfo info, TypeSizes typeSizes, int version)
        {
            long size = DeletionTime.serializer.serializedSize(info.topLevel, typeSizes);
            if (version < MessagingService.VERSION_12)
                return size;

            return size + itSerializer.serializedSize(info.ranges, typeSizes, version);
        }

        public long serializedSize(DeletionInfo info, int version)
        {
            return serializedSize(info, TypeSizes.NATIVE, version);
        }

        public void serializeForSSTable(DeletionInfo info, DataOutput dos) throws IOException
        {
            serialize(info, dos, 0);
        }

        public DeletionInfo deserializeFromSSTable(DataInput dis, Descriptor.Version version) throws IOException
        {
            return deserialize(dis, 0);
        }

        public long serializedSizeForSSTable(DeletionInfo info)
        {
            return serializedSize(info, 0);
        }
    }
}
