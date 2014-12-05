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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * General interface for storage-engine read queries.
 */
public class PartitionRangeReadCommand extends ReadCommand implements Pageable
{
    private final DataRange dataRange;

    public PartitionRangeReadCommand(CFMetaData metadata,
                                     int nowInSec,
                                     ColumnFilter columnFilter,
                                     DataLimits limits,
                                     DataRange dataRange)
    {
        super(metadata, nowInSec, columnFilter, limits);
        this.dataRange = dataRange;
    }

    public DataRange dataRange()
    {
        return dataRange;
    }

    public boolean isNamesQuery()
    {
        return dataRange.isNamesQuery();
    }

    public PartitionRangeReadCommand forSubRange(AbstractBounds<RowPosition> range)
    {
        return new PartitionRangeReadCommand(metadata(), nowInSec(), columnFilter(), limits(), dataRange().forSubRange(range));
    }

    public PartitionRangeReadCommand copy()
    {
        return new PartitionRangeReadCommand(metadata(), nowInSec(), columnFilter(), limits(), dataRange());
    }

    public PartitionRangeReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeReadCommand(metadata(), nowInSec(), columnFilter(), newLimits, dataRange());
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    protected PartitionIterator queryStorage(final ColumnFamilyStore cfs)
    {
        final long start = System.nanoTime();
        final OpOrder.Group op = cfs.readOrdering.start();
        try
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(dataRange().keyRange()));
            Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), dataRange().keyRange().getString(metadata().getKeyValidator()));
            return new WrappingPartitionIterator(getSequentialIterator(view, cfs))
            {
                private boolean closed;

                @Override
                public void close()
                {
                    if (closed)
                        return;

                    try
                    {
                        super.close();
                    }
                    finally
                    {
                        op.close();
                        closed = true;
                        cfs.metric.rangeLatency.addNano(System.nanoTime() - start);
                    }
                }
            };
        }
        catch (RuntimeException e)
        {
            op.close();
            cfs.metric.rangeLatency.addNano(System.nanoTime() - start);
            throw e;
        }
    }

    private PartitionIterator getSequentialIterator(ColumnFamilyStore.ViewFragment view, ColumnFamilyStore cfs)
    {
        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<PartitionIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());

        for (Memtable memtable : view.memtables)
            iterators.add(memtable.makePartitionIterator(dataRange()));

        for (SSTableReader sstable : view.sstables)
            iterators.add(sstable.getScanner(dataRange()));

        return checkCacheFilter(PartitionIterators.merge(iterators, nowInSec()), cfs);
    }

    private PartitionIterator checkCacheFilter(PartitionIterator iter, final ColumnFamilyStore cfs)
    {
        return new WrappingPartitionIterator(iter)
        {
            @Override
            public AtomIterator next()
            {
                AtomIterator next = super.next();
                DecoratedKey dk = next.partitionKey();

                // Check if this partition is in the rowCache and if it is, if  it covers our filter
                CachedPartition cached = cfs.getRawCachedPartition(dk);
                PartitionFilter filter = dataRange().partitionFilter(metadata().comparator, dk);

                if (cached != null && cfs.isFilterFullyCoveredBy(filter, limits(), cached, nowInSec()))
                    return filter.getAtomIterator(cached);

                return next;
            }
        };
    }
}
