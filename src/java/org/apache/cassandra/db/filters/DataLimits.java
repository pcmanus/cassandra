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
package org.apache.cassandra.db.filters;

import java.io.IOException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;

/**
 * Object in charge of tracking if we have fetch enough data for a given query.
 *
 * The reason this is not just a simple integer is that Thrift and CQL3 count
 * stuffs in different ways. This is what abstract those differences.
 */
public abstract class DataLimits
{
    // Please note that this shouldn't be use for thrift
    public static final DataLimits NONE = new CQLLimits(Integer.MAX_VALUE)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            return false;
        }

        @Override
        public PartitionIterator filter(PartitionIterator iter, int nowInSec)
        {
            return iter;
        }

        @Override
        public AtomIterator filter(AtomIterator iter, int nowInSec)
        {
            return iter;
        }
    };

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return new CQLLimits(cqlRowLimit);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return new CQLLimits(cqlRowLimit, perPartitionLimit);
    }

    public static DataLimits thriftLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public abstract DataLimits forPaging(int pageSize);
    public abstract boolean hasEnoughLiveData(CachedPartition cached, int nowInSec);

    // Note that -1 is allowed for nowInSec if we want to count every row (typically
    // if we know everything passed on will be live)
    public abstract RowCounter newRowCounter(int nowInSec);

    // TODO: why do we need that again?
    public abstract int count();
    public abstract boolean countCells();

    public PartitionIterator filter(PartitionIterator iter, int nowInSec)
    {
        return new CountingPartitionIterator(iter, newRowCounter(nowInSec));
    }

    public AtomIterator filter(AtomIterator iter, int nowInSec)
    {
        return new CountingAtomIterator(iter, newRowCounter(nowInSec));
    }

    /**
     * Estimate the number of results (the definition of "results" will be rows for CQL queries
     * and partitions for thrift ones) that a full scan of the provided cfs would yield.
     */
    //if (cfs.metadata.comparator.isDense())
    //{
    //    // one storage row per result row, so use key estimate directly
    //    return cfs.estimateKeys();
    //}
    //else
    //{
    //    float resultRowsPerStorageRow = ((float) cfs.getMeanColumns()) / cfs.metadata.regularColumns().size();
    //    return resultRowsPerStorageRow * (cfs.estimateKeys());
    //}
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public interface RowCounter
    {
        public void newPartition(DecoratedKey partitionKey);
        public void newRow(Row row);

        public int counted();

        public boolean isDone();
        public boolean isDoneForPartition();
    }

    private static class CQLLimits extends DataLimits
    {
        private final int rowLimit;
        private final int perPartitionLimit;

        private CQLLimits(int rowLimit)
        {
            this(rowLimit, Integer.MAX_VALUE);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit)
        {
            this.rowLimit = rowLimit;
            this.perPartitionLimit = perPartitionLimit;
        }

        public DataLimits forPaging(int pageSize)
        {
            return new CQLLimits(pageSize, perPartitionLimit);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            // We want the number of row that are currently live. Getting that
            // precise number force us to iterator the cached partition in general,
            // but we can avoid that if:
            //   - The number of rows with at least one non-expiring cell is
            //     greater than what we ask. We're then fine.
            //   - The number of rows is less than requested, in which case we
            //     know there won't have enough data.
            if (cached.rowsWithNonExpiringCells() >= rowLimit)
                return true;

            if (cached.rowCount() < rowLimit)
                return false;

            // Otherwise, we need to re-count
            AtomIterator cacheIter = cached.atomIterator(cached.columns(), Slices.ALL, false);
            try (CountingAtomIterator iter = new CountingAtomIterator(cacheIter, newRowCounter(nowInSec)))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext() && !iter.counter().isDone())
                    iter.next();
                return iter.counter().isDone();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public RowCounter newRowCounter(int nowInSec)
        {
            if (nowInSec < 0)
                return new LiveCounter();
            else
                return new Counter(nowInSec);
        }

        public int count()
        {
            return rowLimit;
        }

        public boolean countCells()
        {
            return false;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells, which
            // is what getMeanColumns returns)
            float resultRowsPerStorageRow = ((float) cfs.getMeanColumns()) / cfs.metadata.regularColumns().columnCount();
            return resultRowsPerStorageRow * (cfs.estimateKeys());
        }

        private abstract class AbstractCounter implements RowCounter
        {
            protected int rowCounted;
            protected int rowInCurrentPartition;

            public void newPartition(DecoratedKey partitionKey)
            {
                rowInCurrentPartition = 0;
            }

            public int counted()
            {
                return rowCounted;
            }

            public boolean isDone()
            {
                return rowCounted >= rowLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || rowInCurrentPartition >= perPartitionLimit;
            }
        }

        private class LiveCounter extends AbstractCounter
        {
            public void newRow(Row row)
            {
                ++rowCounted;
            }
        }

        private class Counter extends AbstractCounter
        {
            private int nowInSec;

            Counter(int nowInSec)
            {
                this.nowInSec = nowInSec;
            }

            public void newRow(Row row)
            {
                if (Rows.hasLiveData(row, nowInSec))
                    ++rowCounted;
            }
        }
    }
}
