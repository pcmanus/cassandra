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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Static utility methods to create query pagers.
 */
public class QueryPagers
{
    private QueryPagers() {};

    private static int maxQueried(SinglePartitionReadCommand command)
    {
        if (command instanceof SinglePartitionNamesCommand)
        {
            // For names query, we actually can't get more that the number of names asked for
            NamesPartitionFilter filter = ((SinglePartitionNamesCommand)command).partitionFilter();
            int max = filter.maxQueried(command.limits().countCells());
            return Math.min(max, command.limits().count());
        }

        return command.limits().count();
    }

    public static boolean mayNeedPaging(Pageable command, int pageSize)
    {
        if (command instanceof Pageable.ReadCommands)
        {
            List<SinglePartitionReadCommand> commands = ((Pageable.ReadCommands)command).commands;

            // Using long on purpose, as we could overflow otherwise
            long maxQueried = 0;
            for (SinglePartitionReadCommand readCmd : commands)
                maxQueried += maxQueried(readCmd);

            return maxQueried > pageSize;
        }
        else if (command instanceof SinglePartitionReadCommand)
        {
            return maxQueried((SinglePartitionReadCommand)command) > pageSize;
        }
        else
        {
            return ((PartitionRangeReadCommand)command).limits().count() > pageSize;
        }
    }

    private static QueryPager pager(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, boolean local, PagingState state)
    {
        if (command instanceof SinglePartitionNamesCommand)
            return new NamesQueryPager((SinglePartitionNamesCommand)command, consistencyLevel, local);
        else
            return new SliceQueryPager((SinglePartitionSliceCommand)command, consistencyLevel, local, state);
    }

    private static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel, boolean local, PagingState state)
    {
        if (command instanceof Pageable.ReadCommands)
        {
            List<SinglePartitionReadCommand> commands = ((Pageable.ReadCommands)command).commands;
            if (commands.size() == 1)
                return pager(commands.get(0), consistencyLevel, local, state);

            return new MultiPartitionPager(commands, consistencyLevel, local, state);
        }
        else if (command instanceof SinglePartitionReadCommand)
        {
            return pager((SinglePartitionReadCommand)command, consistencyLevel, local, state);
        }
        else
        {
            assert command instanceof PartitionRangeReadCommand;
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand)command;
            if (rangeCommand.isNamesQuery())
                return new RangeNamesQueryPager(rangeCommand, consistencyLevel, local, state);
            else
                return new RangeSliceQueryPager(rangeCommand, consistencyLevel, local, state);
        }
    }

    public static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel)
    {
        return pager(command, consistencyLevel, false, null);
    }

    public static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel, PagingState state)
    {
        return pager(command, consistencyLevel, false, state);
    }

    public static QueryPager localPager(Pageable command)
    {
        return pager(command, null, true, null);
    }

    /**
     * Convenience method that count (live) cells/rows for a given slice of a row, but page underneath.
     */
    // TODO
    //public static int countPaged(String keyspace,
    //                             String columnFamily,
    //                             ByteBuffer key,
    //                             SliceQueryFilter filter,
    //                             ConsistencyLevel consistencyLevel,
    //                             final int pageSize,
    //                             long now) throws RequestValidationException, RequestExecutionException
    //{
    //    SliceFromReadCommand command = new SliceFromReadCommand(keyspace, key, columnFamily, now, filter);
    //    final SliceQueryPager pager = new SliceQueryPager(command, consistencyLevel, false);

    //    ColumnCounter counter = filter.columnCounter(Schema.instance.getCFMetaData(keyspace, columnFamily).comparator, now);
    //    while (!pager.isExhausted())
    //    {
    //        List<Row> next = pager.fetchPage(pageSize);
    //        if (!next.isEmpty())
    //            counter.countAll(next.get(0).cf);
    //    }
    //    return counter.live();
    //}
}
