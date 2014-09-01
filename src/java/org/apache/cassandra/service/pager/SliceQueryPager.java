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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pager over a SliceFromReadCommand.
 */
public class SliceQueryPager extends AbstractQueryPager implements SinglePartitionPager
{
    private static final Logger logger = LoggerFactory.getLogger(SliceQueryPager.class);

    private final SinglePartitionSliceCommand command;

    private volatile ClusteringPrefix lastReturned;

    // Don't use directly, use QueryPagers method instead
    SliceQueryPager(SinglePartitionSliceCommand command, ConsistencyLevel consistencyLevel, boolean localQuery)
    {
        super(consistencyLevel, localQuery, command.metadata(), command.limits());
        this.command = command;
    }

    SliceQueryPager(SinglePartitionSliceCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        this(command, consistencyLevel, localQuery);

        if (state != null)
        {
            lastReturned = cfm.layout().decodeCellName(state.cellName).left;
            restoreState(state.remaining, true);
        }
    }

    public ByteBuffer key()
    {
        return command.partitionKey().getKey();
    }

    public DataLimits limits()
    {
        return command.limits();
    }

    public PagingState state()
    {
        return lastReturned == null
             ? null
             : new PagingState(null, cfm.layout().encodeCellName(lastReturned, null), maxRemaining());
    }

    protected DataIterator queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestValidationException, RequestExecutionException
    {
        SlicePartitionFilter filter = command.partitionFilter();
        if (lastReturned != null)
            filter = filter.withUpdatedStart(command.metadata().comparator, lastReturned);

        logger.debug("Querying next page of slice query; new filter: {}", filter);
        SinglePartitionReadCommand pageCmd = new SinglePartitionSliceCommand(command.metadata(),
                                                                             command.nowInSec(),
                                                                             command.columnFilter(),
                                                                             command.limits().forPaging(pageSize),
                                                                             command.partitionKey(),
                                                                             filter);
        return localQuery
             ? PartitionIterators.asDataIterator(pageCmd.executeLocally(getStore()), command.nowInSec())
             : StorageProxy.read(Collections.singletonList(pageCmd), consistencyLevel);
    }

    protected boolean shouldSkip(DecoratedKey key, Row row)
    {
        return Objects.equal(lastReturned, row.clustering());
    }

    protected boolean recordLast(DecoratedKey key, Row last)
    {
        lastReturned = last.clustering().takeAlias();
        return true;
    }
}
