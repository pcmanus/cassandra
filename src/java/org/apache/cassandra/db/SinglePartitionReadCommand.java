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
import java.util.UUID;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * General interface for storage-engine read queries.
 */
public abstract class SinglePartitionReadCommand<F extends PartitionFilter> extends ReadCommand
{
    private final DecoratedKey partitionKey;
    private final F partitionFilter;

    protected SinglePartitionReadCommand(CFMetaData metadata,
                                         int nowInSec,
                                         ColumnFilter columnFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         F partitionFilter)
    {
        super(metadata, nowInSec, columnFilter, limits);
        this.partitionKey = partitionKey;
        this.partitionFilter = partitionFilter;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public F partitionFilter()
    {
        return partitionFilter;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    protected PartitionIterator queryStorage(final ColumnFamilyStore cfs)
    {
        final long start = System.nanoTime();
        try
        {
            AtomIterator result;

            if (cfs.isRowCacheEnabled())
            {
                assert !cfs.isIndex(); // CASSANDRA-5732
                result = getThroughCache(cfs);
            }
            else
            {
                result = queryMemtableAndDisk(cfs);
            }

            return new SingletonPartitionIterator(result)
            {
                @Override
                public void close()
                {
                    try
                    {
                        super.close();
                    }
                    finally
                    {
                        cfs.metric.readLatency.addNano(System.nanoTime() - start);
                    }
                }
            };
        }
        catch (RuntimeException e)
        {
            cfs.metric.readLatency.addNano(System.nanoTime() - start);
            throw e;
        }
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     *
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     *
     * If the partition is is not cached, we figure out what filter is "biggest", read
     * that from disk, then filter the result and either cache that or return it.
     */
    private AtomIterator getThroughCache(ColumnFamilyStore cfs)
    {
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [" + cfs.name + "]");

        UUID cfId = metadata().cfId;
        RowCacheKey key = new RowCacheKey(cfId, partitionKey());

        // Attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire partitions on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                cfs.metric.rowCacheMiss.inc();
                return queryMemtableAndDisk(cfs);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(partitionFilter(), limits(), cachedPartition, nowInSec()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                return partitionFilter().getAtomIterator(cachedPartition);
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return queryMemtableAndDisk(cfs);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        boolean cacheFullPartitions = metadata().getCaching().rowCache.cacheFullPartitions();

        // To be able to cache what we read, what we read must at least covers what the cache holds, that
        // is the 'rowsToCache' first rows of the partition. We could read those 'rowsToCache' first rows
        // systematically, but we'd have to "extend" that to whatever is needed for the user query that the
        // 'rowsToCache' first rows don't cover and it's not trivial with our existing filters. So currently
        // we settle for caching what we read only if the user query does query the head of the partition since
        // that's the common case of when we'll be able to use the cache anyway. One exception is if we cache
        // full partitions, in which case we just always read it all and cache.
        if (cacheFullPartitions || partitionFilter().isHeadFilter())
        {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
            boolean sentinelReplaced = false;

            try
            {
                int rowsToCache = cacheFullPartitions ? Integer.MAX_VALUE : metadata().getCaching().rowCache.rowsToCache;
                AtomIterator iter = ReadCommands.fullPartitionRead(metadata(), partitionKey(), nowInSec()).queryMemtableAndDisk(cfs);
                try
                {
                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = ArrayBackedPartition.create(DataLimits.cqlLimits(rowsToCache).filter(iter, nowInSec()));
                    if (sentinelSuccess && !toCache.isEmpty())
                    {
                        Tracing.trace("Caching {} rows", toCache.rowCount());
                        CacheService.instance.rowCache.replace(key, sentinel, toCache);
                        // Whether or not the previous replace has worked, our sentinel is not in the cache anymore
                        sentinelReplaced = true;
                    }

                    // We then re-filter out what this query wants.
                    // Note that in the case where we don't cache full partitions, it's possible that the current query is interested in more
                    // than what we've cached, so we can't just use toCache.
                    AtomIterator cacheIterator = partitionFilter().getAtomIterator(toCache);
                    return cacheFullPartitions
                         ? cacheIterator
                         : AtomIterators.concat(cacheIterator, partitionFilter().filter(iter));
                }
                catch (RuntimeException e)
                {
                    iter.close();
                    throw e;
                }
            }
            finally
            {
                if (sentinelSuccess && !sentinelReplaced)
                    cfs.invalidateCachedPartition(key);
            }
        }

        Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
        return queryMemtableAndDisk(cfs);
    }

    private static final AtomicInteger counter = new AtomicInteger();

    public AtomIterator queryMemtableAndDisk(ColumnFamilyStore cfs)
    {
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        boolean copyOnHeap = Memtable.MEMORY_POOL.needToCopyOnHeap();
        final OpOrder.Group op = cfs.readOrdering.start();
        final int c = counter.getAndIncrement();
        System.err.println("Taken OP  " + c);
        return new WrappingAtomIterator(queryMemtableAndDiskInternal(cfs, copyOnHeap))
        {
            private boolean closed;

            @Override
            public void close()
            {
                // Make sure we don't close twice as this would confuse OpOrder
                if (closed)
                    return;

                try
                {
                    super.close();
                }
                finally
                {
                    System.err.println("Release OP " + c);
                    op.close();
                    closed = true;
                }
            }
        };
    }

    // TODO
    public abstract SinglePartitionReadCommand maybeGenerateRetryCommand(DataResolver resolver, Row row);

    protected abstract AtomIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap);
}
