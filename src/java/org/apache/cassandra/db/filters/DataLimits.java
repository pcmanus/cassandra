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
    // TODO
    public static final DataLimits NONE = null;

    public static DataLimits cqlLimits(int cqlRowLimit, boolean isDistinct)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static DataLimits thriftLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public DataLimits forPaging(int pageSize)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Estimate the number of results (the definition of "results" will be rows for CQL queries
     * and partitions for thrift ones) that a full scan of the provided cfs would yield.
     */
    public float estimateTotalResults(ColumnFamilyStore cfs)
    {
        // TODO
        throw new UnsupportedOperationException();
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
    }

    public abstract boolean hasEnoughData(CachedPartition cached, int nowInSec);

    public abstract PartitionIterator filter(PartitionIterator iter);

    public abstract AtomIterator filter(AtomIterator iter);

    public abstract RowCounter newRowCounter();

    public abstract int count();

    public abstract boolean countCells();

    public interface RowCounter
    {
        public void newPartition(DecoratedKey partitionKey);
        public void newRow(Row row);
        public void partitionDone();

        public int counted();
        public boolean isDone();
    }
}
