package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.*;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Index implementation which indexes the values for a single column in the base
 * table and which stores its index data in a local, hidden table.
 */
public class CassandraIndex implements Index
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndex.class);

    protected ColumnFamilyStore baseCfs;
    private ColumnIndexMetadata metadata;

    public void init(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public IndexMetadata getIndexMetadata()
    {
        return metadata.indexMetadata;
    }

    public String getIndexName()
    {
        return metadata.getIndexName();
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return metadata == null ? Optional.empty() : Optional.of(metadata.indexCfs);
    }

    public Callable<Void> getBlockingFlushTask()
    {
        return () -> {
            metadata.indexCfs.forceBlockingFlush();
            return null;
        };
    }

    public Callable<?> getInvalidateTask()
    {
        return () -> {
            markRemoved();
            invalidate();
            return null;
        };
    }

    public Callable<?> getMetadataReloadTask()
    {
        return () -> {
            metadata.indexCfs.metadata.reloadIndexMetadataProperties(metadata.baseCfs.metadata);
            metadata.indexCfs.reload();
            return null;
        };
    }

    public Callable<?> getTruncateTask(final long truncatedAt)
    {
        return () -> {
            metadata.indexCfs.discardSSTables(truncatedAt);
            return null;
        };
    }

    public Callable<?> setIndexMetadata(IndexMetadata indexDef)
    {
        ColumnIndexFunctions functions = ColumnIndexFunctions.getFunctions(baseCfs.metadata, indexDef);
        CFMetaData cfm = functions.indexCfsMetadata(baseCfs.metadata, indexDef);
        ColumnFamilyStore indexCfs =
            ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                      cfm.cfName,
                                                      cfm,
                                                      baseCfs.getTracker().loadsstables);
        // todo - assert indexDef has only 1 target column & extract it for use here
        metadata = new ColumnIndexMetadata(indexDef, baseCfs, indexCfs, indexDef.indexedColumn(baseCfs.metadata), functions);
        // if we're just linking in the index on an already-built index post-restart, we're done
        // Otherwise, we create a stripped down Indexer, which contains only the newly added
        // ColumnIndexer and submit for building via SecondaryIndexBuilder
        return isBuilt() ? null : getBuildIndexTask();
    }

    public boolean indexes(PartitionColumns columns)
    {
        // if we have indexes on the partition key or clustering columns, return true
        return isPrimaryKeyIndex() || columns.contains(metadata.indexedColumn);
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        return metadata.indexedColumn.name.equals(column.name)
               && metadata.functions.supportsOperator(metadata.indexedColumn, operator);
    }

    private boolean supportsExpression(RowFilter.Expression expression)
    {
        return supportsExpression(expression.column(), expression.operator());
    }

    public long getEstimatedResultRows()
    {
        return metadata.estimateResultRows();
    }

    /**
     * No post processing of query results, just return them unchanged
     */
    public BiFunction<PartitionIterator, RowFilter, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, rowFilter) -> partitionIterator;
    }

    public Optional<RowFilter> getReducedFilter(RowFilter filter)
    {
        return getTargetExpression(filter.getExpressions()).map(filter::without);
    }

    private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions)
    {
        return expressions.stream().filter(this::supportsExpression).findFirst();
    }

    public Index.Searcher searcherFor(ReadCommand command)
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            target.get().validateForIndexing();
            return new CassandraIndexSearcher(metadata, command, target.get(), this);
        }

        return null;

    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        switch (metadata.indexedColumn.kind)
        {
            case PARTITION_KEY:
                validatePartitionKey(update.partitionKey());
                break;
            case CLUSTERING:
                validateClusterings(update);
                break;
            case REGULAR:
                validateRows(update);
                break;
            case STATIC:
                validateRows(Collections.singleton(update.staticRow()));
                break;
        }
    }

    public Indexer indexerFor(final DecoratedKey key,
                              final int nowInSec,
                              final OpOrder.Group opGroup,
                              final SecondaryIndexManager.TransactionType transactionType)
    {
        return new Indexer()
        {
            public void insertRow(Row row)
            {
                if (isPrimaryKeyIndex())
                {
                    indexPrimaryKey(row.clustering(),
                                    getPrimaryKeyIndexLiveness(row),
                                    row.deletion());
                }
                else
                {
                    if (metadata.indexedColumn.isComplex())
                        indexCells(row.clustering(), row.getComplexColumnData(metadata.indexedColumn));
                    else
                        indexCell(row.clustering(), row.getCell(metadata.indexedColumn));
                }
            }

            public void removeRow(Row row)
            {
                if (isPrimaryKeyIndex())
                    indexPrimaryKey(row.clustering(), row.primaryKeyLivenessInfo(), row.deletion());

                if (metadata.indexedColumn.isComplex())
                    removeCells(row.clustering(), row.getComplexColumnData(metadata.indexedColumn));
                else
                    removeCell(row.clustering(), row.getCell(metadata.indexedColumn));
            }

            public void updateRow(Row oldRow, Row newRow)
            {
                if (isPrimaryKeyIndex())
                    indexPrimaryKey(newRow.clustering(),
                                    newRow.primaryKeyLivenessInfo(),
                                    newRow.deletion());

                if (metadata.indexedColumn.isComplex())
                {
                    indexCells(newRow.clustering(), newRow.getComplexColumnData(metadata.indexedColumn));
                    removeCells(oldRow.clustering(), oldRow.getComplexColumnData(metadata.indexedColumn));
                }
                else
                {
                    indexCell(newRow.clustering(), newRow.getCell(metadata.indexedColumn));
                    removeCell(oldRow.clustering(), oldRow.getCell(metadata.indexedColumn));
                }
            }

            private void indexCells(Clustering clustering, Iterable<Cell> cells)
            {
                if (cells == null)
                    return;

                for (Cell cell : cells)
                    indexCell(clustering, cell);
            }

            private void indexCell(Clustering clustering, Cell cell)
            {
                if (cell == null)
                    return;

                insert(key.getKey(),
                       clustering,
                       cell,
                       LivenessInfo.create(cell.timestamp(), cell.ttl(), cell.localDeletionTime()),
                       opGroup);
            }

            private void removeCells(Clustering clustering, Iterable<Cell> cells)
            {
                if (cells == null)
                    return;

                for (Cell cell : cells)
                    removeCell(clustering, cell);
            }

            private void removeCell(Clustering clustering, Cell cell)
            {
                if (cell == null)
                    return;

                delete(key.getKey(), clustering, cell, opGroup, nowInSec);
            }

            private void indexPrimaryKey(final Clustering clustering,
                                         final LivenessInfo liveness,
                                         final DeletionTime deletion)
            {
                if (liveness.timestamp() != LivenessInfo.NO_TIMESTAMP)
                    insert(key.getKey(), clustering, null, liveness, opGroup);

                if (!deletion.isLive())
                    delete(key.getKey(), clustering, deletion, opGroup);
            }

            private LivenessInfo getPrimaryKeyIndexLiveness(Row row)
            {
                long timestamp = row.primaryKeyLivenessInfo().timestamp();
                int ttl = row.primaryKeyLivenessInfo().ttl();
                for (Cell cell : row.cells())
                {
                    long cellTimestamp = cell.timestamp();
                    if (cell.isLive(nowInSec))
                    {
                        if (cellTimestamp > timestamp)
                        {
                            timestamp = cellTimestamp;
                            ttl = cell.ttl();
                        }
                    }
                }
                return LivenessInfo.create(baseCfs.metadata, timestamp, ttl, nowInSec);
            }
        };
    }

    /**
     * Specific to internal indexes, this is called by a
     * searcher when it encounters a stale entry in the index
     * @param indexKey the partition key in the index table
     * @param indexClustering the clustering in the index table
     * @param deletion deletion timestamp etc
     * @param opGroup the operation under which to perform the deletion
     */
    public void deleteStaleEntry(DecoratedKey indexKey,
                                 Clustering indexClustering,
                                 DeletionTime deletion,
                                 OpOrder.Group opGroup)
    {
        doDelete(indexKey, indexClustering, deletion, opGroup);
        logger.debug("Removed index entry for stale value {}", indexKey);
    }

    /**
     * Called when adding a new entry to the index
     */
    private void insert(ByteBuffer rowKey,
                        Clustering clustering,
                        Cell cell,
                        LivenessInfo info,
                        OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               cell));
        Row row = BTreeRow.noCellLiveRow(buildIndexClustering(rowKey, clustering, cell), info);
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        metadata.indexCfs.apply(upd, SecondaryIndexManager.IndexTransaction.NO_OP, opGroup, null);
        logger.debug("Inserted entry into index for value {}", valueKey);
    }

    /**
     * Called when deleting entries on non-primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering clustering,
                        Cell cell,
                        OpOrder.Group opGroup,
                        int nowInSec)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               cell));
        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, cell),
                 new DeletionTime(cell.timestamp(), nowInSec),
                 opGroup);
    }

    /**
     * Called when deleting entries from indexes on primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering clustering,
                        DeletionTime deletion,
                        OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               null));
        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, null),
                 deletion,
                 opGroup);
    }

    private void doDelete(DecoratedKey indexKey,
                          Clustering indexClustering,
                          DeletionTime deletion,
                          OpOrder.Group opGroup)
    {
        Row row = BTreeRow.emptyDeletedRow(indexClustering, deletion);
        PartitionUpdate upd = partitionUpdate(indexKey, row);
        metadata.indexCfs.apply(upd, SecondaryIndexManager.IndexTransaction.NO_OP, opGroup, null);
        logger.debug("Removed index entry for value {}", indexKey);
    }

    private void validatePartitionKey(DecoratedKey partitionKey) throws InvalidRequestException
    {
        assert metadata.indexedColumn.isPartitionKey();
        validateIndexedValue(getIndexedValue(partitionKey.getKey(), null, null ));
    }

    private void validateClusterings(PartitionUpdate update) throws InvalidRequestException
    {
        assert metadata.indexedColumn.isClusteringColumn();
        for (Row row : update)
            validateIndexedValue(getIndexedValue(null, row.clustering(), null));
    }

    private void validateRows(Iterable<Row> rows)
    {
        assert !metadata.indexedColumn.isPrimaryKeyColumn();
        for (Row row : rows)
        {
            if (metadata.indexedColumn.isComplex())
            {
                ComplexColumnData data = row.getComplexColumnData(metadata.indexedColumn);
                if (data != null)
                {
                    for (Cell cell : data)
                    {
                        validateIndexedValue(getIndexedValue(null, null, cell.path(), cell.value()));
                    }
                }
            }
            else
            {
                validateIndexedValue(getIndexedValue(null, null, row.getCell(metadata.indexedColumn)));
            }
        }
    }

    private void validateIndexedValue(ByteBuffer value)
    {
        if (value != null && value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Cannot index value of size %d for index %s on %s.%s(%s) (maximum allowed size=%d)",
                                                            value.remaining(),
                                                            metadata.getIndexName(),
                                                            metadata.baseCfs.metadata.ksName,
                                                            metadata.baseCfs.metadata.cfName,
                                                            metadata.indexedColumn.name.toString(),
                                                            FBUtilities.MAX_UNSIGNED_SHORT));
    }

    private ByteBuffer getIndexedValue(ByteBuffer rowKey,
                                       Clustering clustering,
                                       Cell cell)
    {
        return metadata.functions.getIndexedValue(metadata,
                                                  rowKey,
                                                  clustering,
                                                  cell == null ? null : cell.path(),
                                                  cell == null ? null : cell.value()
        );
    }

    private ByteBuffer getIndexedValue(ByteBuffer rowKey,
                                       Clustering clustering,
                                       CellPath path,
                                       ByteBuffer value)
    {
        return metadata.functions.getIndexedValue(metadata, rowKey, clustering, path, value);
    }

    private Clustering buildIndexClustering(ByteBuffer rowKey,
                                            Clustering clustering,
                                            Cell cell)
    {
        return metadata.functions.buildIndexClusteringPrefix(metadata,
                                                             rowKey,
                                                             clustering,
                                                             cell == null ? null : cell.path()).build();
    }

    private DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        return metadata.indexCfs.decorateKey(value);
    }

    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row)
    {
        return PartitionUpdate.singleRowUpdate(metadata.indexCfs.metadata, valueKey, row);
    }

    private void invalidate()
    {
        // interrupt in-progress compactions
        Collection<ColumnFamilyStore> cfss = Collections.singleton(metadata.indexCfs);
        CompactionManager.instance.interruptCompactionForCFs(cfss, true);
        CompactionManager.instance.waitForCessation(cfss);
        metadata.indexCfs.keyspace.writeOrder.awaitNewBarrier();
        metadata.indexCfs.forceBlockingFlush();
        metadata.indexCfs.readOrdering.awaitNewBarrier();
        metadata.indexCfs.invalidate();
    }

    private boolean isBuilt()
    {
        return SystemKeyspace.isIndexBuilt(metadata.baseCfs.keyspace.getName(),
                                           metadata.getIndexName());
    }

    private void markBuilt()
    {
        SystemKeyspace.setIndexBuilt(metadata.baseCfs.keyspace.getName(),
                                     metadata.getIndexName());
    }

    private void markRemoved()
    {
        SystemKeyspace.setIndexRemoved(metadata.baseCfs.keyspace.getName(),
                                       metadata.getIndexName());
    }

    private boolean isPrimaryKeyIndex()
    {
        return metadata.indexedColumn.isPrimaryKeyColumn();
    }

    private Callable<?> getBuildIndexTask()
    {
        return () -> {
            buildBlocking();
            return null;
        };
    }

    private void buildBlocking()
    {
        baseCfs.forceBlockingFlush();

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.select(SSTableSet.CANONICAL));
             Refs<SSTableReader> sstables = viewFragment.refs)
        {
            if (sstables.isEmpty())
            {
                logger.info("No SSTable data for {}.{} to build index {} from, marking empty index as built",
                            baseCfs.metadata.ksName,
                            baseCfs.metadata.cfName,
                            metadata.getIndexName());
                markBuilt();
                return;
            }

            logger.info("Submitting index build of {} for data in {}",
                        metadata.getIndexName(),
                        getSSTableNames(sstables));

            SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs,
                                                                      Collections.singleton(this),
                                                                      new ReducingKeyIterator(sstables));
            Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
            FBUtilities.waitOnFuture(future);
            metadata.indexCfs.forceBlockingFlush();
            markBuilt();
        }
        logger.info("Index build of {} complete", metadata.getIndexName());
    }

    private static String getSSTableNames(Collection<SSTableReader> sstables)
    {
        return StreamSupport.stream(sstables.spliterator(), false)
                            .map(SSTableReader::toString)
                            .collect(Collectors.joining(", "));
    }
}
