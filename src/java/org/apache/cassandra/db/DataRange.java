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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.dht.*;

/**
 * Groups both the range of partitions to query, and the partition filter to
 * apply for each partition (for a (partition) range query).
 *
 * The main "trick" is that the partition filter can only be obtained by
 * providing the partition key on which the filter will be applied. This is
 * necessary when paging range queries, as we might want a different filter
 * for the starting and stopping key than for other keys (because the previous
 * page we had queried may have ended in the middle of a partition).
 */
public class DataRange
{
    private final AbstractBounds<RowPosition> keyRange;
    protected final PartitionFilter partitionFilter;

    public DataRange(AbstractBounds<RowPosition> range, PartitionFilter partitionFilter)
    {
        this.keyRange = range;
        this.partitionFilter = partitionFilter;
    }

    public static DataRange allData(CFMetaData metadata, IPartitioner partitioner)
    {
        return forKeyRange(metadata, new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
    }

    public static DataRange forKeyRange(CFMetaData metadata, Range<Token> keyRange)
    {
        return new DataRange(keyRange.toRowBounds(), PartitionFilters.fullPartitionFilter(metadata));
    }

    public AbstractBounds<RowPosition> keyRange()
    {
        return keyRange;
    }

    public boolean isNamesQuery()
    {
        return partitionFilter instanceof NamesPartitionFilter;
    }

    public RowPosition startKey()
    {
        return keyRange.left;
    }

    public RowPosition stopKey()
    {
        return keyRange.right;
    }

    // Whether the bounds of this DataRange actually wraps around.
    public boolean isWrapAround()
    {
        // On range can ever wrap
        return keyRange instanceof Range && ((Range)keyRange).isWrapAround();
    }

    public boolean contains(RowPosition pos)
    {
        return keyRange.contains(pos);
    }

    public PartitionFilter partitionFilter(ClusteringComparator comparator, DecoratedKey key)
    {
        return partitionFilter;
    }

    public DataRange forPaging(AbstractBounds<RowPosition> range, ClusteringPrefix firstPartitionStart, ClusteringPrefix lastPartitionEnd)
    {
        return new Paging(range, partitionFilter, firstPartitionStart, lastPartitionEnd);
    }

    private static class Paging extends DataRange
    {
        private final ClusteringPrefix firstPartitionStart;
        private final ClusteringPrefix lastPartitionEnd;

        private Paging(AbstractBounds<RowPosition> range, PartitionFilter filter, ClusteringPrefix firstPartitionStart, ClusteringPrefix lastPartitionEnd)
        {
            super(range, filter);

            // When using a paging range, we don't allow wrapped ranges, as it's unclear how to handle them properly.
            // This is ok for now since we only need this in range queries, and the range are "unwrapped" in that case.
            assert !(range instanceof Range) || !((Range)range).isWrapAround() || range.right.isMinimum() : range;

            this.firstPartitionStart = firstPartitionStart;
            this.lastPartitionEnd = lastPartitionEnd;
        }

        @Override
        public PartitionFilter partitionFilter(ClusteringComparator comparator, DecoratedKey key)
        {
            PartitionFilter filter = partitionFilter;
            if (firstPartitionStart != null && key.equals(startKey()))
                filter = filter.withUpdatedStart(comparator, firstPartitionStart);
            if (lastPartitionEnd != null && key.equals(stopKey()))
                filter = filter.withUpdatedEnd(comparator, lastPartitionEnd);
            return filter;
        }
    }
}
