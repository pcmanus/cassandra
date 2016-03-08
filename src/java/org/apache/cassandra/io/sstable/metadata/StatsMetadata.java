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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.sstable.format.Version;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * SSTable metadata that always stay on heap.
 */
public class StatsMetadata extends MetadataComponent
{
    public static final IMetadataComponentSerializer serializer = new StatsMetadataSerializer();

    public final EstimatedHistogram estimatedPartitionSize;
    public final EstimatedHistogram estimatedColumnCount;
    public final ReplayPosition replayPosition;
    public final long minTimestamp;
    public final long maxTimestamp;
    public final int minPurgingTime;
    public final int maxPurgingTime;
    public final int minPurgingReferenceTime;
    public final int maxPurgingReferenceTime;
    public final int minTTL;
    public final int maxTTL;
    public final double compressionRatio;
    public final StreamingHistogram estimatedTombstonePurgingTime;
    public final int sstableLevel;
    public final List<ByteBuffer> minClusteringValues;
    public final List<ByteBuffer> maxClusteringValues;
    public final boolean hasLegacyCounterShards;
    public final long repairedAt;
    public final long totalColumnsSet;
    public final long totalRows;

    public StatsMetadata(EstimatedHistogram estimatedPartitionSize,
                         EstimatedHistogram estimatedColumnCount,
                         ReplayPosition replayPosition,
                         long minTimestamp,
                         long maxTimestamp,
                         int minPurgingTime,
                         int maxPurgingTime,
                         int minPurgingReferenceTime,
                         int maxPurgingReferenceTime,
                         int minTTL,
                         int maxTTL,
                         double compressionRatio,
                         StreamingHistogram estimatedTombstonePurgingTime,
                         int sstableLevel,
                         List<ByteBuffer> minClusteringValues,
                         List<ByteBuffer> maxClusteringValues,
                         boolean hasLegacyCounterShards,
                         long repairedAt,
                         long totalColumnsSet,
                         long totalRows)
    {
        this.estimatedPartitionSize = estimatedPartitionSize;
        this.estimatedColumnCount = estimatedColumnCount;
        this.replayPosition = replayPosition;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minPurgingTime = minPurgingTime;
        this.maxPurgingTime = maxPurgingTime;
        this.minPurgingReferenceTime = minPurgingReferenceTime;
        this.maxPurgingReferenceTime = maxPurgingReferenceTime;
        this.minTTL = minTTL;
        this.maxTTL = maxTTL;
        this.compressionRatio = compressionRatio;
        this.estimatedTombstonePurgingTime = estimatedTombstonePurgingTime;
        this.sstableLevel = sstableLevel;
        this.minClusteringValues = minClusteringValues;
        this.maxClusteringValues = maxClusteringValues;
        this.hasLegacyCounterShards = hasLegacyCounterShards;
        this.repairedAt = repairedAt;
        this.totalColumnsSet = totalColumnsSet;
        this.totalRows = totalRows;
    }

    public MetadataType getType()
    {
        return MetadataType.STATS;
    }

    /**
     * @param nowInSec the current local time in seconds
     * @return estimated droppable tombstone ratio.
     */
    public double getEstimatedDroppableTombstoneRatio(int nowInSec)
    {
        long estimatedColumnCount = this.estimatedColumnCount.mean() * this.estimatedColumnCount.count();
        if (estimatedColumnCount > 0)
        {
            double droppable = getDroppableTombstones(nowInSec);
            return droppable / estimatedColumnCount;
        }
        return 0.0f;
    }

    /**
     * @param nowInSec the current time in seconds
     * @return The amount of droppable tombstones in the sstable.
     */
    public double getDroppableTombstones(int nowInSec)
    {
        return estimatedTombstonePurgingTime.sum(nowInSec);
    }

    public StatsMetadata mutateLevel(int newLevel)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedColumnCount,
                                 replayPosition,
                                 minTimestamp,
                                 maxTimestamp,
                                 minPurgingTime,
                                 maxPurgingTime,
                                 minPurgingReferenceTime,
                                 maxPurgingReferenceTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstonePurgingTime,
                                 newLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 repairedAt,
                                 totalColumnsSet,
                                 totalRows);
    }

    public StatsMetadata mutateRepairedAt(long newRepairedAt)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedColumnCount,
                                 replayPosition,
                                 minTimestamp,
                                 maxTimestamp,
                                 minPurgingTime,
                                 maxPurgingTime,
                                 minPurgingReferenceTime,
                                 maxPurgingReferenceTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstonePurgingTime,
                                 sstableLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 newRepairedAt,
                                 totalColumnsSet,
                                 totalRows);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsMetadata that = (StatsMetadata) o;
        return new EqualsBuilder()
                       .append(estimatedPartitionSize, that.estimatedPartitionSize)
                       .append(estimatedColumnCount, that.estimatedColumnCount)
                       .append(replayPosition, that.replayPosition)
                       .append(minTimestamp, that.minTimestamp)
                       .append(maxTimestamp, that.maxTimestamp)
                       .append(minPurgingTime, that.minPurgingTime)
                       .append(maxPurgingTime, that.maxPurgingTime)
                       .append(minPurgingReferenceTime, that.minPurgingReferenceTime)
                       .append(maxPurgingReferenceTime, that.maxPurgingReferenceTime)
                       .append(minTTL, that.minTTL)
                       .append(maxTTL, that.maxTTL)
                       .append(compressionRatio, that.compressionRatio)
                       .append(estimatedTombstonePurgingTime, that.estimatedTombstonePurgingTime)
                       .append(sstableLevel, that.sstableLevel)
                       .append(repairedAt, that.repairedAt)
                       .append(maxClusteringValues, that.maxClusteringValues)
                       .append(minClusteringValues, that.minClusteringValues)
                       .append(hasLegacyCounterShards, that.hasLegacyCounterShards)
                       .append(totalColumnsSet, that.totalColumnsSet)
                       .append(totalRows, that.totalRows)
                       .build();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                       .append(estimatedPartitionSize)
                       .append(estimatedColumnCount)
                       .append(replayPosition)
                       .append(minTimestamp)
                       .append(maxTimestamp)
                       .append(minPurgingTime)
                       .append(maxPurgingTime)
                       .append(minPurgingReferenceTime)
                       .append(maxPurgingReferenceTime)
                       .append(minTTL)
                       .append(maxTTL)
                       .append(compressionRatio)
                       .append(estimatedTombstonePurgingTime)
                       .append(sstableLevel)
                       .append(repairedAt)
                       .append(maxClusteringValues)
                       .append(minClusteringValues)
                       .append(hasLegacyCounterShards)
                       .append(totalColumnsSet)
                       .append(totalRows)
                       .build();
    }

    public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata>
    {
        public int serializedSize(Version version, StatsMetadata component) throws IOException
        {
            int size = 0;
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedPartitionSize);
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedColumnCount);
            size += ReplayPosition.serializer.serializedSize(component.replayPosition);
            if (version.storeRows())
                size += 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8; // mix/max timestamp(long), min/maxPurgingTime(int), min/max TTL, compressionRatio(double), repairedAt (long)
            else
                size += 8 + 8 + 4 + 8 + 8; // mix/max timestamp(long), maxLocalDeletionTime(int), compressionRatio(double), repairedAt (long)
            size += StreamingHistogram.serializer.serializedSize(component.estimatedTombstonePurgingTime);
            size += TypeSizes.sizeof(component.sstableLevel);
            // min column names
            size += 4;
            for (ByteBuffer value : component.minClusteringValues)
                size += 2 + value.remaining(); // with short length
            // max column names
            size += 4;
            for (ByteBuffer value : component.maxClusteringValues)
                size += 2 + value.remaining(); // with short length
            size += TypeSizes.sizeof(component.hasLegacyCounterShards);
            if (version.storeRows())
                size += 8 + 8; // totalColumnsSet, totalRows
            if (version.hasPurgingReferenceTimeStatistics())
                size += 4 + 4; // min/max purgingReferenceTime
            return size;
        }

        public void serialize(Version version, StatsMetadata component, DataOutputPlus out) throws IOException
        {
            EstimatedHistogram.serializer.serialize(component.estimatedPartitionSize, out);
            EstimatedHistogram.serializer.serialize(component.estimatedColumnCount, out);
            ReplayPosition.serializer.serialize(component.replayPosition, out);
            out.writeLong(component.minTimestamp);
            out.writeLong(component.maxTimestamp);
            if (version.storeRows())
                out.writeInt(component.minPurgingTime);
            out.writeInt(component.maxPurgingTime);
            if (version.storeRows())
            {
                out.writeInt(component.minTTL);
                out.writeInt(component.maxTTL);
            }
            out.writeDouble(component.compressionRatio);
            StreamingHistogram.serializer.serialize(component.estimatedTombstonePurgingTime, out);
            out.writeInt(component.sstableLevel);
            out.writeLong(component.repairedAt);
            out.writeInt(component.minClusteringValues.size());
            for (ByteBuffer value : component.minClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeInt(component.maxClusteringValues.size());
            for (ByteBuffer value : component.maxClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeBoolean(component.hasLegacyCounterShards);

            if (version.storeRows())
            {
                out.writeLong(component.totalColumnsSet);
                out.writeLong(component.totalRows);
            }
            if (version.hasPurgingReferenceTimeStatistics())
            {
                out.writeInt(component.minPurgingReferenceTime);
                out.writeInt(component.maxPurgingReferenceTime);
            }
        }

        public StatsMetadata deserialize(Version version, DataInputPlus in) throws IOException
        {
            EstimatedHistogram partitionSizes = EstimatedHistogram.serializer.deserialize(in);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
            ReplayPosition replayPosition = ReplayPosition.serializer.deserialize(in);
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();
            // We use MAX_VALUE as that's the default value for "no purging time"
            int minPurgingTime = version.storeRows() ? in.readInt() : Integer.MAX_VALUE;
            int maxPurgingTime = in.readInt();
            int minTTL = version.storeRows() ? in.readInt() : 0;
            int maxTTL = version.storeRows() ? in.readInt() : Integer.MAX_VALUE;
            double compressionRatio = in.readDouble();
            StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
            int sstableLevel = in.readInt();
            long repairedAt = 0;
            if (version.hasRepairedAt())
                repairedAt = in.readLong();

            int colCount = in.readInt();
            List<ByteBuffer> minClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
                minClusteringValues.add(ByteBufferUtil.readWithShortLength(in));

            colCount = in.readInt();
            List<ByteBuffer> maxClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
                maxClusteringValues.add(ByteBufferUtil.readWithShortLength(in));

            boolean hasLegacyCounterShards = true;
            if (version.tracksLegacyCounterShards())
                hasLegacyCounterShards = in.readBoolean();

            long totalColumnsSet = version.storeRows() ? in.readLong() : -1L;
            long totalRows = version.storeRows() ? in.readLong() : -1L;

            // Using MAX_VALUE in both case because that's our "no purging time"
            int minPurgingReferenceTime = version.hasPurgingReferenceTimeStatistics() ? in.readInt() : Integer.MAX_VALUE;
            int maxPurgingReferenceTime = version.hasPurgingReferenceTimeStatistics() ? in.readInt() : Integer.MAX_VALUE;

            return new StatsMetadata(partitionSizes,
                                     columnCounts,
                                     replayPosition,
                                     minTimestamp,
                                     maxTimestamp,
                                     minPurgingTime,
                                     maxPurgingTime,
                                     minPurgingReferenceTime,
                                     maxPurgingReferenceTime,
                                     minTTL,
                                     maxTTL,
                                     compressionRatio,
                                     tombstoneHistogram,
                                     sstableLevel,
                                     minClusteringValues,
                                     maxClusteringValues,
                                     hasLegacyCounterShards,
                                     repairedAt,
                                     totalColumnsSet,
                                     totalRows);
        }
    }
}
