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
package org.apache.cassandra.service.pager;

import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

/**
 * Pages a RangeSliceCommand whose predicate is a slice query.
 *
 * Note: this only work for CQL3 queries for now (because thrift queries expect
 * a different limit on the rows than on the columns, which complicates it).
 */
public class RangeSliceQueryPager extends AbstractQueryPager
{
    private final PartitionRangeReadCommand command;
    private volatile DecoratedKey lastReturnedKey;
    private volatile ClusteringPrefix lastReturnedClustering;

    // Don't use directly, use QueryPagers method instead
    RangeSliceQueryPager(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery)
    {
        super(consistencyLevel, localQuery, command.metadata(), command.limits());
        this.command = command;
        assert !command.isNamesQuery();
    }

    RangeSliceQueryPager(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        this(command, consistencyLevel, localQuery);

        if (state != null)
        {
            lastReturnedKey = StorageService.getPartitioner().decorateKey(state.partitionKey);
            lastReturnedClustering = cfm.layout().decodeCellName(state.cellName).left;
            restoreState(state.remaining, true);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), cfm.layout().encodeCellName(lastReturnedClustering, null), maxRemaining());
    }

    protected DataIterator queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        DataRange dataRange = lastReturnedKey == null
                            ? command.dataRange()
                            : command.dataRange().forPaging(makeIncludingKeyBounds(lastReturnedKey), lastReturnedClustering, null);

        PartitionRangeReadCommand pageCmd = new PartitionRangeReadCommand(command.metadata(),
                                                                          command.nowInSec(),
                                                                          command.columnFilter(),
                                                                          command.limits().forPaging(pageSize),
                                                                          dataRange);

        return localQuery
             ? PartitionIterators.asDataIterator(pageCmd.executeLocally(getStore()), command.nowInSec())
             : StorageProxy.getRangeSlice(pageCmd, consistencyLevel);
    }

    protected boolean shouldSkip(DecoratedKey key, Row row)
    {
        return Objects.equal(lastReturnedKey, key) && Objects.equal(lastReturnedClustering, row.clustering());
    }

    protected boolean recordLast(DecoratedKey key, Row last)
    {
        lastReturnedKey = key;
        lastReturnedClustering = last.clustering().takeAlias();
        return true;
    }

    private AbstractBounds<RowPosition> makeIncludingKeyBounds(RowPosition lastReturnedKey)
    {
        // We always include lastReturnedKey since we may still be paging within a row,
        // and PagedRangeCommand will move over if we're not anyway
        AbstractBounds<RowPosition> bounds = command.dataRange().keyRange();
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return new Bounds<RowPosition>(lastReturnedKey, bounds.right);
        }
        else
        {
            return new IncludingExcludingBounds<RowPosition>(lastReturnedKey, bounds.right);
        }
    }
}
