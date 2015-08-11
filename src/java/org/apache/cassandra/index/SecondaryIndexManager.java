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
package org.apache.cassandra.index;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Handles the core maintenance functionality associated with indexes: adding/removing them to or from
 * a table, (re)building during bootstrap or other streaming operations, flushing, reloading metadata
 * and so on.
 *
 * The Index interface defines a number of methods which return Callable<?>. These are primarily the
 * management tasks for an index implementation. Most of them are currently executed in a blocking
 * fashion via submission to SIM's blockingExecutor. This provides the desired behaviour in pretty
 * much all cases, as tasks like flushing an index needs to be executed synchronously to avoid potentially
 * deadlocking on the FlushWriter or PostFlusher. Several of these Callable<?> returning methods on Index could
 * then be defined with as void and called directly from SIM (rather than being run via the executor service).
 * Separating the task defintion from execution gives us greater flexibility though, so that in future, for example,
 * if the flush process allows it we leave open the possibility of executing more of these tasks asynchronously.
 *
 * The primary exception to the above is the Callable returned from Index#addIndexedColumn. This may
 * involve a significant effort, building a new index over any existing data. We perform this task asynchronously;
 * as it is called as part of a schema update, which we do not want to block for a long period. Building non-custom
 * indexes is performed on the CompactionManager.
 *
 * This class also provides instances of processors which listen to updates to the base table and forward to
 * registered Indexes the info required to keep those indexes up to date.
 * There are two variants of these processors, each with a factory method provided by SIM:
 *      IndexTransaction: deals with updates generated on the regular write path.
 *      CleanupTransaction: used when partitions are modified during compaction or cleanup operations.
 * Further details on their usage and lifecycles can be found in the interface definitions below.
 *
 * Finally, the bestIndexFor method is used at query time to identify the most selective index of those able
 * to satisfy any search predicates defined by a ReadCommand's RowFilter. It returns a thin IndexAccessor object
 * which enables the ReadCommand to access the appropriate functions of the Index at various stages in its lifecycle.
 * e.g. the getEstimatedResultRows is required when StorageProxy calculates the initial concurrency factor for
 * distributing requests to replicas, whereas a Searcher instance is needed when the ReadCommand is executed locally on
 * a target replica.
 */
public class SecondaryIndexManager implements IndexRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    /**
     * Used to differentiate between type of index transaction when obtaining
     * a handler from Index implementations.
     */
    public enum TransactionType { WRITE_TIME, COMPACTION, CLEANUP }

    private Set<Index> indexes = Sets.newConcurrentHashSet();

    // executes tasks returned by Indexer#addIndexColumn which may require index(es) to be (re)built
    private static final ExecutorService asyncExecutor =
        new JMXEnabledThreadPoolExecutor(1,
                                         StageManager.KEEPALIVE,
                                         TimeUnit.SECONDS,
                                         new LinkedBlockingQueue<>(),
                                         new NamedThreadFactory("SecondaryIndexManagement"),
                                         "internal");

    // executes all blocking tasks produced by Indexers e.g. getFlushTask, getMetadataReloadTask etc
    private static final ExecutorService blockingExecutor = MoreExecutors.newDirectExecutorService();


    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    /**
     * Drops and adds new indexes associated with the underlying CF
     */
    public void reload()
    {
        // ensure sure every registered Indexer knows not to process any un-indexed columns
        baseCfs.metadata.allColumns()
                        .stream()
                        .filter(column -> column.getIndexType() == null)
                        .forEach(this::removeIndexedColumn);

        // re-add any columns which do have indexing options set
        baseCfs.metadata.allColumns()
                        .stream()
                        .filter(column -> column.getIndexType() != null)
                        .forEach(this::addIndexedColumn);

        executeAllBlocking(indexes.stream(), Index::getMetadataReloadTask);
    }

    /**
     * Called when dropping a Table
     */
    public void markAllIndexesRemoved()
    {
       getBuiltIndexNames().forEach(this::markIndexRemoved);
    }

    /**
    * Does a full, blocking rebuild of the indexes specified by columns from the sstables.
    * Caller must acquire and release references to the sstables used here.
    * Note also that only this method of (re)building indexes:
    *   a) takes a set of index *names* rather than Indexers
    *   b) marks exsiting indexes removed prior to rebuilding
    *
    * @param sstables the data to build from
    * @param indexNames the list of indexes to be rebuilt
    */
    public void rebuildIndexesBlocking(Collection<SSTableReader> sstables, Set<String> indexNames)
    {
        Set<Index> toRebuild = StreamSupport.stream(indexes.spliterator(), false)
                                              .filter(indexer -> indexNames.contains(indexer.getIndexName()))
                                              .collect(Collectors.toSet());
        if (toRebuild.isEmpty())
        {
            logger.info("No defined indexes with the supplied names");
            return;
        }

        toRebuild.forEach(indexer -> markIndexRemoved(indexer.getIndexName()));

        buildIndexesBlocking(sstables, toRebuild);

        toRebuild.forEach(indexer -> markIndexBuilt(indexer.getIndexName()));
    }

    public void buildAllIndexesBlocking(Collection<SSTableReader> sstables)
    {
        buildIndexesBlocking(sstables, ImmutableSet.copyOf(indexes));
    }

    // For convenience, may be called directly from Index impls
    public void buildIndexBlocking(Index index)
    {
        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.select(SSTableSet.CANONICAL));
             Refs<SSTableReader> sstables = viewFragment.refs)
        {
            buildIndexesBlocking(sstables, Collections.singleton(index));
            markIndexBuilt(index.getIndexName());
        }
    }

    private void buildIndexesBlocking(Collection<SSTableReader> sstables, Set<Index> indexes)
    {
        if (indexes.isEmpty())
            return;

        // todo use index names, not indexes toString
        logger.info("Submitting index build of {} for data in {}",
                    indexes.stream().map(Index::getIndexName).collect(Collectors.joining(",")),
                    sstables.stream().map(SSTableReader::toString).collect(Collectors.joining(",")));

        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs,
                                                                  indexes,
                                                                  new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        FBUtilities.waitOnFuture(future);

        flushIndexesBlocking(indexes);
        // todo names here also
        logger.info("Index build of {} complete",
                    indexes.stream().map(Index::getIndexName).collect(Collectors.joining(",")));
    }

    private void markIndexBuilt(String indexName)
    {
        SystemKeyspace.setIndexBuilt(baseCfs.name, indexName);
    }

    private void markIndexRemoved(String indexName)
    {
        SystemKeyspace.setIndexRemoved(baseCfs.name, indexName);
    }

    /**
     * Removes a existing index
     * @param column the indexed column to remove
     */
    public void removeIndexedColumn(final ColumnDefinition column)
    {
        // Indexer impls are responsible for unregistering themselves
        // and any associated IndexSearcher.Factory when they are
        // no longer needed
        executeAllBlocking(indexes.stream(), (index) -> index.removeIndexedColumn(column));
        indexes.forEach(index -> index.maybeUnregister(this));
    }

    /**
     * Adds and builds a index for a column
     * @param cdef the column definition holding the index data
     */
    // todo will change when we decouple index / column
    public synchronized Future<?> addIndexedColumn(ColumnDefinition cdef)
    {
        assert cdef.getIndexType() != null;

        Index index = findIndex(cdef);

        if (!index.getIndexedColumns().contains(cdef))
        {
            // add the new column to the indexer and maybe trigger a rebuild
            final Callable<?> rebuildTask = index.addIndexedColumn(cdef);
            index.register(this);
            return rebuildTask == null
                   ? Futures.immediateFuture(null)
                   : asyncExecutor.submit(rebuildTask);
        }

        return Futures.immediateFuture(null);
    }

    private Index findIndex(ColumnDefinition column)
    {
        if (column.getIndexType() != IndexType.CUSTOM)
            return findInternalIndex(column);
        else
            return findCustomIndex(column);
    }

    // todo this will change when we update syntax to allow index creation for a row, without a column def
    private Index findCustomIndex(ColumnDefinition indexedColumn)
    {
        assert indexedColumn.getIndexOptions() != null;
        String className = indexedColumn.getIndexOptions().get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
        assert className != null;
        // for now, we only allow a single instance of a custom index on a table - this is to
        // temporarily emulate the behaviour of the old PRSI. It will go away with true per-row indexes
        return indexes.stream()
                      .filter(i -> i.getClass().getName().equals(className))
                      .findFirst()
                      .orElseGet(() -> {
                          try
                          {
                              Index newIndex = (Index) FBUtilities.classForName(className, "Index").newInstance();
                              newIndex.init(baseCfs);
                              return newIndex;
                          }
                          catch (Exception e)
                          {
                              throw new RuntimeException(e);
                          }
                      });
    }

    private Index findInternalIndex(ColumnDefinition cdef)
    {
        return indexes.stream()
                      .filter(i -> i instanceof CassandraIndex && i.getIndexedColumns().contains(cdef))
                      .findFirst()
                      .orElseGet(() -> {
                          CassandraIndex indexer = new CassandraIndex();
                          indexer.init(baseCfs);
                          return indexer;
                      });
    }

    /**
     * Truncate all indexes
     */
    public void truncateAllIndexesBlocking(final long truncatedAt)
    {
        executeAllBlocking(indexes.stream(), (index) -> index.getTruncateTask(truncatedAt));
    }

    /**
     * Remove all indexes
     */
    public void invalidateAllIndexesBlocking()
    {
        executeAllBlocking(indexes.stream(), Index::getInvalidateTask);
    }

    /**
     * Perform a blocking flush all indexes
     */
    public void flushAllIndexesBlocking()
    {
       flushIndexesBlocking(ImmutableSet.copyOf(indexes));
    }

    /**
     * Perform a blocking flush of selected indexes
     */
    public void flushIndexesBlocking(Set<Index> indexes)
    {
        if (indexes.isEmpty())
            return;

        synchronized (baseCfs.getTracker())
        {
            executeAllBlocking(StreamSupport.stream(indexes.spliterator(), false),
                               Index::getBlockingFlushTask);
        }
    }

    /**
     * Performs a blocking flush of all custom indexes
     */
    public void flushAllCustomIndexesBlocking()
    {
        Set<Index> customIndexers = indexes.stream()
                                             .filter(index -> !(index instanceof CassandraIndex))
                                             .collect(Collectors.toSet());
        flushIndexesBlocking(customIndexers);
    }

    /**
     * @return all indexes which are marked as built and ready to use
     */
    public List<String> getBuiltIndexNames()
    {
        Set<String> allIndexNames = new HashSet<>();
        indexes.stream()
                .map(Index::getIndexName)
                .forEach(allIndexNames::add);
        return SystemKeyspace.getBuiltIndexes(baseCfs.keyspace.getName(), allIndexNames);
    }

    /**
     * @return all backing Tables used by registered indexes
     */
    public Set<ColumnFamilyStore> getAllIndexStorageTables()
    {
        Set<ColumnFamilyStore> backingTables = new HashSet<>();
        indexes.forEach(index -> index.getBackingTable().ifPresent(backingTables::add));
        return backingTables;
    }

    /**
     * @return if there are ANY indexes registered for this table
     */
    public boolean hasIndexes()
    {
        return !indexes.isEmpty();
    }

    /**
     * When building an index against existing data in sstables, add the given partition to the index
     */
    public void indexPartition(UnfilteredRowIterator partition, OpOrder.Group opGroup, Set<Index> indexes, int nowInSec)
    {
        if (!indexes.isEmpty())
        {
            DecoratedKey key = partition.partitionKey();
            Set<Index.Indexer> indexers = indexes.stream()
                                                 .map(index -> index.indexerFor(key,
                                                                                nowInSec,
                                                                                opGroup,
                                                                                TransactionType.WRITE_TIME))
                                                 .collect(Collectors.toSet());

            indexers.forEach(Index.Indexer::begin);

            if (!partition.staticRow().isEmpty())
                indexers.forEach(indexer -> indexer.insertRow(partition.staticRow()));

            try (RowIterator filtered = UnfilteredRowIterators.filter(partition, nowInSec))
            {
                while (filtered.hasNext())
                {
                    Row row = filtered.next();
                    indexers.forEach(indexer -> indexer.insertRow(row));
                }
            }

            indexers.forEach(Index.Indexer::finish);
        }
    }

    /**
     * Delete all data from all indexes for this partition.
     * For when cleanup rips a partition out entirely.
     *
     * TODO : improve cleanup transaction to batch updates & perform them async
     */
    public void deletePartition(UnfilteredRowIterator partition, int nowInSec)
    {
        CleanupTransaction indexTransaction = newCleanupTransaction(partition.partitionKey(),
                                                                    partition.columns(),
                                                                    nowInSec,
                                                                    TransactionType.CLEANUP);
        indexTransaction.start(1);
        indexTransaction.onPartitionDeletion(partition.partitionLevelDeletion());
        indexTransaction.commit();

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered.kind() != Unfiltered.Kind.ROW)
                continue;

            indexTransaction = newCleanupTransaction(partition.partitionKey(),
                                                     partition.columns(),
                                                     nowInSec,
                                                     TransactionType.CLEANUP);
            indexTransaction.start(1);
            indexTransaction.onRowDelete((Row)unfiltered);
            indexTransaction.commit();
        }
    }

    /**
     * Called at query time to find the most selective of the registered index implementation
     * (i.e. the one likely to return the fewest results) from those registered.
     * Implementation specific validation of the target expression by the most selective
     * index should be performed in the searcherFor method to ensure that we pick the right
     * index regardless of the validity of the expression.
     *
     * This method is called at various points during the lifecycle of a ReadCommand.
     * Ideally, we would do this relatively expensive operation only once, and attach the index to the
     * ReadCommand for future reference. This requires the index be passed onto additional commands generated
     * to process subranges etc.
     *
     * @param command ReadCommand to be executed
     * @return a Searcher instance, ready to use during execution of the command, or null if none
     * of the registered indexes can support the command.
     */
    public IndexAccessor getBestIndexFor(ReadCommand command, boolean includeInTrace)
    {
        if (indexes.isEmpty() || command.rowFilter().isEmpty())
            return null;

        // Group the registered indexes by the size of the reduced filter they produce,
        // filtering out any which cannot reduce the intial filter.
        // if there are no results where the reduced filter size < the initial filter, then
        // none of the indexes can be used for this command. If there are useable indexes,
        // then we pick the one with the highest estimated result count

        Multimap<Integer, Index> indexesByReducedFilterSize =
            Multimaps.newListMultimap(new TreeMap<>(Ints::compare), ArrayList::new);

        indexes.forEach(index -> index.getReducedFilter(command.rowFilter())
                                      .ifPresent(reduced ->
                                          indexesByReducedFilterSize.put(reduced.getExpressions().size(), index)
                                      )
        );

        // no indexes were able to reduce the filter
        if (indexesByReducedFilterSize.isEmpty())
        {
            logger.debug("No applicable indexes found");
            Tracing.trace("No applicable indexes found");
            return null;
        }

        // the list of indexes which reduce the initial filter to smallest resulting list of expressions to filter by
        List<Index> mostReducing = (List<Index>)indexesByReducedFilterSize.asMap()
                                                                          .entrySet()
                                                                          .iterator()
                                                                          .next()
                                                                          .getValue();

        // we have multiple indexes which reduce the filter by the same degree
        // pick whichever provides the lowest estimated result set
        if (mostReducing.size() > 1)
            Collections.sort(mostReducing, (i1, i2) -> Longs.compare(i1.getEstimatedResultRows(),
                                                                     i2.getEstimatedResultRows()));

        // pay for an additional threadlocal get() rather than build the strings unnecessarily
        if (includeInTrace && Tracing.isTracing())
        {
            Tracing.trace("Candidate indexes reduce the inital row filter to a list of {} expressions. " +
                          "Index mean cardinalities are {}. " +
                          "Scanning with {}.",
                          indexesByReducedFilterSize.asMap().keySet().iterator().next(),
                          mostReducing.stream().map(i -> i.getIndexName() + ':' + i.getEstimatedResultRows())
                                      .collect(Collectors.joining(",")),
                          mostReducing.get(0).getIndexName());
        }
        return new IndexAccessor(mostReducing.get(0), command);
    }

    // convenience method which doesn't emit tracing messages
    public IndexAccessor getBestIndexFor(ReadCommand command)
    {
        return getBestIndexFor(command, false);
    }

    /**
     * In most circumstances only the Index itself is required. This is true when we simply want to obtain
     * the estimated result size (in StorageProxy#estimateResultsPerRange), when we want to get a function to
     * post process results of a query on the coordinator (in PartitionRangeReadCommand#postReconciliationProcessing)
     * or when initializing a ReadOrderGroup for a query involving internal indexes (in ReadOrderGroup#maybeGetIndexCfs).
     * On the other hand, when a Searcher instance is required (to actually perform an index lookup in
     * ReadCommand#executeLocally), we also need access to the specific RowFilter.Expression that was used to
     * select the Index. This is a simple wrapper to encapsulate that and provide access to what external callers
     * actually need (and not to what they don't).
     */
    public static class IndexAccessor
    {
        final Index index;
        final ReadCommand command;

        private IndexAccessor(Index index, ReadCommand command)
        {
            this.index = index;
            this.command = command;
        }

        public Index.Searcher getSearcher()
        {
            return index.searcherFor(command);
        }

        public BiFunction<PartitionIterator, RowFilter, PartitionIterator> postProcessor()
        {
            return index.postProcessorFor(command);
        }

        public long estimateResultRows()
        {
            return index.getEstimatedResultRows();
        }

        public Optional<ColumnFamilyStore> getBackingTable()
        {
            return index.getBackingTable();
        }

        public RowFilter getReducedFilter()
        {
            // we wouldn't have created this accessor if the wrapped index were incapable of
            // reducing the command's row filter, but to be on the safe side we include a
            // fallback to returning the initial filter as it
            return index.getReducedFilter(command.rowFilter()).orElse(command.rowFilter());
        }
    }

    /**
     * Called at write time to ensure that values which are valid for use
     * as a PartitionKey in a primary table is also valid as an indexed
     * value in any registered index.
     * @param partitionKey
     * @throws InvalidRequestException
     */
    public void validate(DecoratedKey partitionKey) throws InvalidRequestException
    {
        indexes.forEach(indexer -> indexer.validate(partitionKey));
    }

    /**
     * Called at write time to ensure that values which are valid for use
     * as a Clustering in a primary table is also valid as an indexed
     * value in any registered index.
     * @param clustering
     * @throws InvalidRequestException
     */
    public void validate(Clustering clustering) throws InvalidRequestException
    {
        indexes.forEach(indexer -> indexer.validate(clustering));
    }

    /**
     * Called at write time to ensure that values which are valid for use
     * as a cell value in a primary table is also valid as an indexed
     * value in any registered index.
     * @param column
     * @param value
     * @param path
     * @throws InvalidRequestException
     */
    public void validate(ColumnDefinition column, ByteBuffer value, CellPath path) throws InvalidRequestException
    {
        indexes.stream()
                .filter(indexer -> indexer.indexes(PartitionColumns.of(column)))
                .forEach(indexer -> indexer.validate(column, value, path));
    }

    /**
     * IndexRegistry methods
     * TODO : maybe do this via composition for testability
     */
    public void registerIndex(Index index)
    {
        indexes.add(index);
    }

    public void unregisterIndex(Index index)
    {
        indexes.remove(index);
    }

    public Collection<Index> listIndexers()
    {
        return ImmutableSet.copyOf(indexes);
    }


    /**
     * This helper acts as a closure around the indexManager and updated data
     * to ensure that down in Memtable's ColumnFamily implementation, the index
     * can get updated.
     */
    public IndexTransaction newUpdateTransaction(PartitionUpdate update, OpOrder.Group opGroup, int nowInSec)
    {
        // todo : optimize lookup, we can probably cache quite a bit of stuff, rather than doing
        // a linear scan every time. Holding off that though until CASSANDRA-7771 to figure out
        // exactly how indexes are to be identified & associated with a given partition update
        Index.Indexer[] indexers = indexes.stream()
                                          .filter(i -> i.indexes(update.columns()))
                                          .map(i -> i.indexerFor(update.partitionKey(),
                                                                 nowInSec,
                                                                 opGroup,
                                                                 TransactionType.WRITE_TIME))
                                          .toArray(Index.Indexer[]::new);

        return indexers.length == 0 ? IndexTransaction.NO_OP : new WriteTimeTransaction(indexers);
    }

    /**
     * Handling of index updates
     */
    public interface IndexTransaction
    {
        // Instances of an IndexTransaction are scoped to a single partition update
        // A new instance is used for every write, obtained from the
        // newUpdateTransaction(PartitionUpdate) method. Likewise, a single
        // CleanupTransaction instance is used for each partition processed during a
        // compaction or cleanup.
        //
        // We make certain guarantees about the lifecycle of each IndexTransaction
        // instance. Namely that start() will be called before any other
        // method, and commit() will be called at the end of the update.
        // Each instance is initialized with 1..many Index.Indexer
        // instances, one per registered Index. As with the IndexTransaction itself,
        // these are scoped to a specific partition update, so implementations
        // can be assured that all indexing events they receive relate to
        // the same single operation.
        //
        // onPartitionDelete(), onRangeTombstone(), onInserted() and onUpdated()
        // calls may arrive in any order, but this should have no impact for the
        // Indexers being notified as any events delivered to a single instance
        // necessarily relate to a single partition.
        //
        // The typical sequence of events during a Memtable update would be:
        // start()                       -- no-op, used to notify Indexers of the start of the transaction
        // onPartitionDeletion(dt)       -- if the PartitionUpdate implies one
        // onRangeTombstone(rt)*         -- for each in the PartitionUpdate, if any
        //
        // then:
        // onInserted(row)*              -- called for each Row not already present in the Memtable
        // onUpdated(existing, updated)* -- called for any Row in the update for where a version was already present
        //                                  in the Memtable. It's important to note here that existing is the previous
        //                                  row from the Memtable & updated is the final version replacing it. It is
        //                                  *not* the incoming row, but the result of merging the incoming and existing
        //                                  rows.
        // commit()                      -- finally, finish is called when the new Partition is swapped into the Memtable

        void start();
        void onPartitionDeletion(DeletionTime deletionTime);
        void onRangeTombstone(RangeTombstone rangeTombstone);
        void onInserted(Row row);
        void onUpdated(Row existing, Row updated);
        void commit();

        IndexTransaction NO_OP = new IndexTransaction()
        {
            public void start(){}
            public void onPartitionDeletion(DeletionTime deletionTime){}
            public void onRangeTombstone(RangeTombstone rangeTombstone){}
            public void onInserted(Row row){}
            public void onUpdated(Row existing, Row updated){}
            public void commit(){}
        };
    }

    /**
     * A single use transaction for processing a partition update on the regular write path
     */
    private static final class WriteTimeTransaction implements IndexTransaction
    {
        private final Index.Indexer[] indexers;

        private WriteTimeTransaction(Index.Indexer...indexers)
        {
            // don't allow null indexers, if we don't need any use a NullUpdater object
            for (Index.Indexer indexer : indexers) assert indexer != null;
            this.indexers = indexers;
        }

        public void start()
        {
            Arrays.stream(indexers).forEach(Index.Indexer::begin);
        }

        public void onPartitionDeletion(DeletionTime deletionTime)
        {
            Arrays.stream(indexers).forEach(h -> h.partitionDelete(deletionTime));
        }

        public void onRangeTombstone(RangeTombstone tombstone)
        {
            Arrays.stream(indexers) .forEach(h -> h.rangeTombstone(tombstone));
        }

        public void onInserted(Row row)
        {
            Arrays.stream(indexers).forEach(h -> h.insertRow(row));
        }

        public void onUpdated(Row existing, Row updated)
        {
            final Row.Builder toRemove = BTreeBackedRow.sortedBuilder(existing.columns());
            toRemove.newRow(existing.clustering());
            final Row.Builder toInsert = BTreeBackedRow.sortedBuilder(updated.columns());
            toInsert.newRow(updated.clustering());
            // diff listener collates the columns to be added & removed from the indexes
            RowDiffListener diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                {
                }

                public void onDeletion(int i, Clustering clustering, DeletionTime merged, DeletionTime original)
                {
                }

                public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                {
                    if (merged != null && merged != original)
                        toInsert.addCell(merged);

                    if (merged == null || (original != null && shouldCleanupOldValue(original, merged)))
                        toRemove.addCell(original);

                }
            };
            Rows.diff(diffListener, updated, updated.columns().mergeTo(existing.columns()), existing);
            Row oldRow = toRemove.build();
            Row newRow = toInsert.build();
            Arrays.stream(indexers).forEach(i -> i.updateRow(oldRow, newRow));
        }

        public void commit()
        {
            Arrays.stream(indexers).forEach(Index.Indexer::finish);
        }

        private boolean shouldCleanupOldValue(Cell oldCell, Cell newCell)
        {
            // If either the value or timestamp is different, then we
            // should delete from the index. If not, then we can infer that
            // at least one of the cells is an ExpiringColumn and that the
            // difference is in the expiry time. In this case, we don't want to
            // delete the old value from the index as the tombstone we insert
            // will just hide the inserted value.
            // Completely identical cells (including expiring columns with
            // identical ttl & localExpirationTime) will not get this far due
            // to the oldCell.equals(newCell) in StandardUpdater.update
            return !oldCell.value().equals(newCell.value()) || oldCell.timestamp() != newCell.timestamp();
        }
    }

    /**
     * Updated closure with only the modified row key
     */
    public CleanupTransaction newCleanupTransaction(DecoratedKey key,
                                                    PartitionColumns partitionColumns,
                                                    int nowInSec,
                                                    TransactionType transactionType)
    {
        Index[] interestedIndexes = indexes.stream()
                                           .filter(i -> i.indexes(partitionColumns))
                                           .toArray(Index[]::new);

        return interestedIndexes.length == 0
               ? CleanupTransaction.NO_OP
               : new IndexGCTransaction(key, nowInSec, transactionType, interestedIndexes);
    }

    public interface CleanupTransaction
    {
        // Compaction & Cleanup are somewhat simpler than dealing with incoming writes,
        // being only concerned with cleaning up stale index entries.
        //
        // When multiple versions of a row are compacted, the CleanupTransaction is
        // notified of the versions being merged, which it diffs against the merge result
        // and forwards to the registered Index.Indexer instances when on commit.

        void start(int versions);
        void onPartitionDeletion(DeletionTime deletionTime);
        void onRowMerge(Columns columns, Row merged, Row...versions);
        void onRowDelete(Row row);
        void commit();

        CleanupTransaction NO_OP = new CleanupTransaction()
        {
            public void start(int versions){}
            public void onPartitionDeletion(DeletionTime deletionTime){}
            public void onRowMerge(Columns columns, Row merged, Row...versions){}
            public void onRowDelete(Row row){}
            public void commit(){}
        };
    }

    /**
     * A single-use transaction for updating indexes for a single partition during a compaction / cleanup operation
     * TODO : make this smarter at batching updates so we can use a single transaction to process multiple rows in
     * a single partition
     */
    private final class IndexGCTransaction implements CleanupTransaction
    {
        private final DecoratedKey key;
        private final int nowInSec;
        private final Index[] indexes;
        private final TransactionType transactionType;

        private Row[] rows;
        private DeletionTime partitionDelete;

        private IndexGCTransaction(DecoratedKey key,
                                   int nowInSec,
                                   TransactionType transactionType,
                                   Index...indexes)
        {
            // don't allow null indexers, if we don't have any, use a noop transaction
            for (Index index : indexes) assert index != null;

            this.key = key;
            this.indexes = indexes;
            this.nowInSec = nowInSec;
            this.transactionType = transactionType;
        }

        public void start(int versions)
        {
            // versions may be 0 if this transaction is only to cover a partition delete during cleanup
            if (versions > 0)
                rows = new Row[versions];
        }

        // Called during cleanup
        public void onPartitionDeletion(DeletionTime deletionTime)
        {
            partitionDelete = deletionTime;
        }

        // Called during cleanup, where versions should always == 1
        public void onRowDelete(Row row)
        {
            assert rows.length == 1;
            rows[0] = row;
        }

        // Called during compaction
        public void onRowMerge(Columns columns, Row merged, Row...versions)
        {
            // Diff listener constructs rows representing deltas between the merged and original versions
            // These delta rows are then passed to registered indexes for removal processing
            final Row.Builder[] builders = new Row.Builder[versions.length];
            RowDiffListener diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                {
                }

                public void onDeletion(int i, Clustering clustering, DeletionTime merged, DeletionTime original)
                {
                }

                public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                {
                    if (original != null && merged != original)
                    {
                        if (builders[i] == null)
                        {
                            builders[i] = BTreeBackedRow.sortedBuilder(columns);
                            builders[i].newRow(clustering);
                        }
                        builders[i].addCell(original);
                    }
                }
            };

            Rows.diff(diffListener, merged, columns, versions);

            for(int i = 0; i < builders.length; i++)
                if (builders[i] != null)
                    rows[i] = builders[i].build();
        }

        public void commit()
        {
            if (rows == null && partitionDelete == null)
                return;

            try (OpOrder.Group opGroup = Keyspace.writeOrder.start())
            {
                Index.Indexer[] indexers = Arrays.stream(indexes)
                                                 .map(i -> i.indexerFor(key, nowInSec, opGroup, transactionType))
                                                 .toArray(Index.Indexer[]::new);

                Arrays.stream(indexers).forEach(Index.Indexer::begin);

                flushPartitionDelete(indexers);

                for (Row row : rows)
                    if (row != null)
                        Arrays.stream(indexers).forEach(indexer -> indexer.removeRow(row));

                Arrays.stream(indexers).forEach(Index.Indexer::finish);
            }
        }

        private void flushPartitionDelete(Index.Indexer[] indexers)
        {
            if (partitionDelete != null)
            {
                Arrays.stream(indexers).forEach(indexer -> indexer.partitionDelete(partitionDelete));
                partitionDelete = null;
            }
        }
    }

    private static void executeAllBlocking(Stream<Index> indexers, Function<Index, Callable<?>> function)
    {
        List<Future<?>> waitFor = new ArrayList<>();
        indexers.forEach(indexer -> {
            Callable<?> task = function.apply(indexer);
            if (null != task)
                waitFor.add(blockingExecutor.submit(task));
        });
        FBUtilities.waitOnFutures(waitFor);
    }
}
