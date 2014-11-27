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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public abstract class AbstractSimplePerColumnSecondaryIndex extends PerColumnSecondaryIndex
{
    protected ColumnFamilyStore indexCfs;

    // SecondaryIndex "forces" a set of ColumnDefinition. However this class (and thus it's subclass)
    // only support one def per index. So inline it in a field for 1) convenience and 2) avoid creating
    // an iterator each time we need to access it.
    // TODO: we should fix SecondaryIndex API
    protected ColumnDefinition columnDef;

    public void init()
    {
        assert baseCfs != null && columnDefs != null && columnDefs.size() == 1;

        columnDef = columnDefs.iterator().next();

        ClusteringComparator cc = SecondaryIndex.getIndexComparator(baseCfs.metadata, columnDef);
        CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, columnDef, cc);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             indexedCfMetadata.cfName,
                                                             new LocalPartitioner(getIndexKeyComparator()),
                                                             indexedCfMetadata);
    }

    protected AbstractType<?> getIndexKeyComparator()
    {
        return columnDef.type;
    }

    @Override
    public DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        return new BufferDecoratedKey(new LocalToken(getIndexKeyComparator(), value), value);
    }

    @Override
    String indexTypeForGrouping()
    {
        return "_internal_";
    }

    protected abstract ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell);

    protected abstract ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell);

    public void delete(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell, OpOrder.Group opGroup)
    {
        assert columnDef.equals(cell.column());

        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey, clustering, cell));
        ColumnDefinition idxColumn = indexCfs.metadata.compactValueColumn();
        PartitionUpdate upd = new PartitionUpdate(indexCfs.metadata, valueKey, PartitionColumns.of(idxColumn), 1);
        Row.Writer writer = upd.writer(false);
        writer.writeClustering(makeIndexClustering(rowKey, clustering, cell));
        Cells.writeTombstone(idxColumn, cell.timestamp(), writer);
        writer.endOfRow();
        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, upd);
    }

    public void insert(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell, OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey, clustering, cell));

        ColumnDefinition idxColumn = indexCfs.metadata.compactValueColumn();
        PartitionUpdate upd = new PartitionUpdate(indexCfs.metadata, valueKey, PartitionColumns.of(idxColumn), 1);
        Row.Writer writer = upd.writer(false);
        writer.writeClustering(makeIndexClustering(rowKey, clustering, cell));
        writer.writeCell(idxColumn, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, cell.timestamp(), cell.localDeletionTime(), cell.ttl(), null);
        writer.endOfRow();
        if (logger.isDebugEnabled())
            logger.debug("applying index row {} in {}", indexCfs.metadata.getKeyValidator().getString(valueKey.getKey()), upd);

        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);
    }

    public void update(ByteBuffer rowKey, ClusteringPrefix clustering, Cell oldCell, Cell cell, OpOrder.Group opGroup)
    {
        // insert the new value before removing the old one, so we never have a period
        // where the row is invisible to both queries (the opposite seems preferable); see CASSANDRA-5540                    
        insert(rowKey, clustering, cell, opGroup);
        if (SecondaryIndexManager.shouldCleanupOldValue(oldCell, cell))
            delete(rowKey, clustering, oldCell, opGroup);
    }

    public void removeIndex(ByteBuffer columnName)
    {
        indexCfs.invalidate();
    }

    public void forceBlockingFlush()
    {
        Future<?> wait;
        // we synchronise on the baseCfs to make sure we are ordered correctly with other flushes to the base CFS
        synchronized (baseCfs.getDataTracker())
        {
            wait = indexCfs.forceFlush();
        }
        FBUtilities.waitOnFuture(wait);
    }

    public void invalidate()
    {
        indexCfs.invalidate();
    }

    public void truncateBlocking(long truncatedAt)
    {
        indexCfs.discardSSTables(truncatedAt);
    }

    public ColumnFamilyStore getIndexCfs()
    {
       return indexCfs;
    }

    public String getIndexName()
    {
        return indexCfs.name;
    }

    public void reload()
    {
        indexCfs.metadata.reloadSecondaryIndexMetadata(baseCfs.metadata);
        indexCfs.reload();
    }
    
    public long estimateResultRows()
    {
        return getIndexCfs().getMeanColumns();
    } 
}
