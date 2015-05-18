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
import java.io.IOException;
import java.util.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * A read command that selects a (part of a) single partition.
 */
public abstract class SinglePartitionReadCommand<F extends PartitionFilter> extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DecoratedKey partitionKey;
    private final F partitionFilter;

    protected SinglePartitionReadCommand(boolean isDigest,
                                         boolean isForThrift,
                                         CFMetaData metadata,
                                         int nowInSec,
                                         ColumnFilter columnFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         F partitionFilter)
    {
        super(Kind.SINGLE_PARTITION, isDigest, isForThrift, metadata, nowInSec, columnFilter, limits);
        this.partitionKey = partitionKey;
        this.partitionFilter = partitionFilter;
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param partitionFilter the partition filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand<?> create(CFMetaData metadata,
                                                       int nowInSec,
                                                       ColumnFilter columnFilter,
                                                       DataLimits limits,
                                                       DecoratedKey partitionKey,
                                                       PartitionFilter partitionFilter)
    {
        return create(false, metadata, nowInSec, columnFilter, limits, partitionKey, partitionFilter);
    }

    /**
     * Creates a new read command on a single partition for thrift.
     *
     * @param isForThrift whether the query is for thrift or not.
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param partitionFilter the partition filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand<?> create(boolean isForThrift,
                                                       CFMetaData metadata,
                                                       int nowInSec,
                                                       ColumnFilter columnFilter,
                                                       DataLimits limits,
                                                       DecoratedKey partitionKey,
                                                       PartitionFilter partitionFilter)
    {
        if (partitionFilter instanceof SlicePartitionFilter)
            return new SinglePartitionSliceCommand(false, isForThrift, metadata, nowInSec, columnFilter, limits, partitionKey, (SlicePartitionFilter)partitionFilter);

        assert partitionFilter instanceof NamesPartitionFilter;
        return new SinglePartitionNamesCommand(false, isForThrift, metadata, nowInSec, columnFilter, limits, partitionKey, (NamesPartitionFilter)partitionFilter);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param filter the partition filter to use for the query.
     *
     * @return a newly created read command. The returned command will use no column filter and have no limits.
     */
    public static SinglePartitionReadCommand<?> create(CFMetaData metadata, int nowInSec, DecoratedKey key, PartitionFilter filter)
    {
        return create(metadata, nowInSec, ColumnFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(CFMetaData metadata, int nowInSec, DecoratedKey key)
    {
        return SinglePartitionSliceCommand.create(metadata, nowInSec, key, Slices.ALL);
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public F partitionFilter()
    {
        return partitionFilter;
    }

    public PartitionFilter partitionFilter(DecoratedKey key)
    {
        return partitionFilter;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public ColumnsSelection queriedColumns()
    {
        return partitionFilter().queriedColumns();
    }

    public boolean selects(DecoratedKey partitionKey, Clustering clustering)
    {
        return partitionKey().equals(partitionKey) && partitionFilter().selects(clustering);
    }

    /**
     * Returns a new command suitable to paging from the last returned row.
     *
     * @param lastReturned the last row returned by the previous page. The newly created command
     * will only query row that comes after this (in query order). This can be {@code null} if this
     * is the first page.
     * @param pageSize the size to use for the page to query.
     *
     * @return the newly create command.
     */
    public SinglePartitionReadCommand forPaging(Clustering lastReturned, int pageSize)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        return create(isForThrift(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      limits().forPaging(pageSize),
                      partitionKey(),
                      lastReturned == null ? partitionFilter() : partitionFilter.forPaging(metadata().comparator, lastReturned, false));
    }

    public DataIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
    {
        return StorageProxy.read(Group.one(this), consistency, clientState);
    }

    public SinglePartitionPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState pagingState)
    {
        return getPager(this, consistency, clientState, pagingState, false);
    }

    public SinglePartitionPager getLocalPager()
    {
        return getPager(this, null, null, null, true);
    }

    private static SinglePartitionPager getPager(SinglePartitionReadCommand command, ConsistencyLevel consistency, ClientState clientState, PagingState pagingState, boolean local)
    {
        return new SinglePartitionPager(command, consistency, clientState, local, pagingState);
    }

    protected PartitionIterator queryStorage(final ColumnFamilyStore cfs)
    {
        final long start = System.nanoTime();
        AtomIterator result = null;
        try
        {
            if (cfs.isRowCacheEnabled())
            {
                assert !cfs.isIndex(); // CASSANDRA-5732
                result = getThroughCache(cfs);
            }
            else
            {
                result = queryMemtableAndDisk(cfs);
            }

            return new SingletonPartitionIterator(result, isForThrift())
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
        catch (RuntimeException | Error e)
        {
            if (result != null)
                result.close();

            cfs.metric.readLatency.addNano(System.nanoTime() - start);
            throw e;
        }
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     * <p>
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     * <p>
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
                return partitionFilter().getAtomIterator(cachedPartition, nowInSec());
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
                AtomIterator iter = SinglePartitionReadCommand.fullPartitionRead(metadata(), nowInSec(), partitionKey()).queryMemtableAndDisk(cfs);
                try
                {
                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = ArrayBackedPartition.create(DataLimits.cqlLimits(rowsToCache).filter(iter));
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
                    AtomIterator cacheIterator = partitionFilter().getAtomIterator(toCache, nowInSec());
                    if (cacheFullPartitions)
                    {
                        // Everything is guaranteed to be in 'toCache', we're done with 'iter'
                        assert !iter.hasNext();
                        iter.close();
                        return cacheIterator;
                    }
                    return AtomIterators.concat(cacheIterator, partitionFilter().filter(iter));
                }
                catch (RuntimeException | Error e)
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

    /**
     * Queries both memtable and sstables to fetch the result of this query.
     * <p>
     * Please note that this method:
     *   1) does not check the row cache.
     *   2) does not apply the query limit, nor the column filter (and so ignore 2ndary indexes).
     *      Those are applied in {@link ReadCommand#executeLocally}.
     *   3) does not record some of the read metrics (latency, scanned cells histograms) nor
     *      throws TombstoneOverwhelmingException.
     * It is publicly exposed because there is a few places where that is exactly what we want,
     * but it should be used only where you know you don't need thoses things.
     */
    public AtomIterator queryMemtableAndDisk(ColumnFamilyStore cfs)
    {
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        boolean copyOnHeap = Memtable.MEMORY_POOL.needToCopyOnHeap();
        final OpOrder.Group op = cfs.readOrdering.start();
        try
        {
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
                        op.close();
                        closed = true;
                    }
                }
            };
        }
        catch (RuntimeException | Error e)
        {
            op.close();
            throw e;
        }
    }

    protected abstract AtomIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap);

    @Override
    public String toString()
    {
        return String.format("Read(%s.%s columnFilter=%s limits=%s key=%s filter=%s)",
                             metadata().ksName,
                             metadata().cfName,
                             columnFilter(),
                             limits(),
                             metadata().getKeyValidator().getString(partitionKey().getKey()),
                             partitionFilter.toString(metadata()));
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ");

        sb.append(ColumnDefinition.toCQLString(metadata().partitionKeyColumns())).append(" = ");
        DataRange.appendKeyString(sb, metadata().getKeyValidator(), partitionKey().getKey());

        // We put the column filter first because the partition filter can end by "ORDER BY"
        if (!columnFilter().isEmpty())
            sb.append(" AND ").append(columnFilter());

        String filterString = partitionFilter().toCQLString(metadata());
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        metadata().getKeyValidator().writeValue(partitionKey().getKey(), out);
        PartitionFilter.serializer.serialize(partitionFilter(), out, version);
    }

    protected long selectionSerializedSize(int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        return metadata().getKeyValidator().writtenLength(partitionKey().getKey(), sizes)
             + PartitionFilter.serializer.serializedSize(partitionFilter(), version);
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group implements ReadQuery
    {
        public final List<SinglePartitionReadCommand<?>> commands;
        private final DataLimits limits;

        public Group(List<SinglePartitionReadCommand<?>> commands, DataLimits limits)
        {
            assert !commands.isEmpty();
            this.commands = commands;
            this.limits = limits;
        }

        public static Group one(SinglePartitionReadCommand<?> command)
        {
            return new Group(Collections.<SinglePartitionReadCommand<?>>singletonList(command), command.limits());
        }

        public DataIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
        {
            return StorageProxy.read(this, consistency, clientState);
        }

        public DataLimits limits()
        {
            return limits;
        }

        public CFMetaData metadata()
        {
            return commands.get(0).metadata();
        }

        public DataIterator executeLocally()
        {
            List<DataIterator> partitions = new ArrayList<>(commands.size());
            for (SinglePartitionReadCommand cmd : commands)
                partitions.add(cmd.executeLocally());

            // Because we only have enforce the limit per command, we need to enforce it globally.
            return limits.filter(DataIterators.concat(partitions));
        }

        public QueryPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState pagingState)
        {
            return getPager(consistency, clientState, pagingState, false);
        }

        public QueryPager getLocalPager()
        {
            return getPager(null, null, null, true);
        }

        private QueryPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState pagingState, boolean local)
        {
            if (commands.size() == 1)
                return getPager(commands.get(0), consistency, clientState, pagingState, local);

            return new MultiPartitionPager(commands, consistency, clientState, local, pagingState, limits);
        }

        @Override
        public String toString()
        {
            return commands.toString();
        }
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInput in, int version, boolean isDigest, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, DataLimits limits)
        throws IOException
        {
            DecoratedKey key = StorageService.getPartitioner().decorateKey(metadata.getKeyValidator().readValue(in));
            PartitionFilter filter = PartitionFilter.serializer.deserialize(in, version, metadata);
            if (filter instanceof NamesPartitionFilter)
                return new SinglePartitionNamesCommand(isDigest, isForThrift, metadata, nowInSec, columnFilter, limits, key, (NamesPartitionFilter)filter);
            else
                return new SinglePartitionSliceCommand(isDigest, isForThrift, metadata, nowInSec, columnFilter, limits, key, (SlicePartitionFilter)filter);
        }
    };
}
