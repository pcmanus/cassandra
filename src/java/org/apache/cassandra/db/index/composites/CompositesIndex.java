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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Base class for secondary indexes where composites are involved.
 */
public abstract class CompositesIndex extends AbstractSimplePerColumnSecondaryIndex
{
    private volatile ClusteringComparator indexComparator;

    protected ClusteringComparator getIndexComparator()
    {
        // Yes, this is racy, but doing this more than once is not a big deal, we just want to avoid doing it every time
        // More seriously, we should fix that whole SecondaryIndex API so this can be a final and avoid all that non-sense.
        if (indexComparator == null)
        {
            assert columnDef != null;
            indexComparator = getIndexComparator(baseCfs.metadata, columnDef);
        }
        return indexComparator;
    }

    public static CompositesIndex create(ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return new CompositesIndexOnCollectionValue();
                case SET:
                    return new CompositesIndexOnCollectionKey();
                case MAP:
                    return cfDef.getIndexOptions().containsKey("index_keys")
                         ? new CompositesIndexOnCollectionKey()
                         : new CompositesIndexOnCollectionValue();
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return new CompositesIndexOnClusteringKey();
            case REGULAR:
                return new CompositesIndexOnRegular();
            case PARTITION_KEY:
                return new CompositesIndexOnPartitionKey();
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }

    // Check SecondaryIndex.getIndexComparator if you want to know why this is static
    public static ClusteringComparator getIndexComparator(CFMetaData baseMetadata, ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return CompositesIndexOnCollectionValue.buildIndexComparator(baseMetadata, cfDef);
                case SET:
                    return CompositesIndexOnCollectionKey.buildIndexComparator(baseMetadata, cfDef);
                case MAP:
                    return cfDef.getIndexOptions().containsKey("index_keys")
                         ? CompositesIndexOnCollectionKey.buildIndexComparator(baseMetadata, cfDef)
                         : CompositesIndexOnCollectionValue.buildIndexComparator(baseMetadata, cfDef);
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return CompositesIndexOnClusteringKey.buildIndexComparator(baseMetadata, cfDef);
            case REGULAR:
                return CompositesIndexOnRegular.buildIndexComparator(baseMetadata, cfDef);
            case PARTITION_KEY:
                return CompositesIndexOnPartitionKey.buildIndexComparator(baseMetadata, cfDef);
            //case COMPACT_VALUE:
            //    return CompositesIndexOnCompactValue.buildIndexComparator(baseMetadata, cfDef);
        }
        throw new AssertionError();
    }

    protected abstract ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell);

    public abstract IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexEntryClustering, Cell indexEntry);

    public abstract boolean isStale(IndexedEntry entry, Row row, int nowInSec);

    public void delete(IndexedEntry entry, OpOrder.Group opGroup)
    {
        ColumnDefinition idxColumn = indexCfs.metadata.compactValueColumn();
        RowUpdate row = RowUpdates.create(entry.indexClustering, Columns.of(idxColumn))
                        .addCell(idxColumn, Cells.createTombsone(entry.timestamp));

        PartitionUpdate upd = new PartitionUpdate(indexCfs.metadata, entry.indexValue).add(row);
        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);

        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", entry.indexValue, upd);
    }

    protected AbstractType<?> getExpressionComparator()
    {
        return baseCfs.metadata.getColumnDefinitionComparator(columnDef);
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
    {
        return new CompositesSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        Map<String, String> options = new HashMap<String, String>(columnDef.getIndexOptions());

        // We used to have an option called "prefix_size" so skip it silently for backward compatibility sake.
        options.remove("prefix_size");

        if (columnDef.type.isCollection())
        {
            options.remove("index_values");
            options.remove("index_keys");
        }

        if (!options.isEmpty())
            throw new ConfigurationException("Unknown options provided for COMPOSITES index: " + options.keySet());
    }

    public static class IndexedEntry
    {
        public final DecoratedKey indexValue;
        public final ClusteringPrefix indexClustering;
        public final long timestamp;

        public final ByteBuffer indexedKey;
        public final ClusteringPrefix indexedEntryClustering;
        public final ByteBuffer indexedEntryCollectionKey; // may be null

        public IndexedEntry(DecoratedKey indexValue, ClusteringPrefix indexClustering, long timestamp, ByteBuffer indexedKey, ClusteringPrefix indexedEntryClustering)
        {
            this(indexValue, indexClustering, timestamp, indexedKey, indexedEntryClustering, null);
        }

        public IndexedEntry(DecoratedKey indexValue,
                            ClusteringPrefix indexClustering,
                            long timestamp,
                            ByteBuffer indexedKey,
                            ClusteringPrefix indexedEntryClustering,
                            ByteBuffer indexedEntryCollectionKey)
        {
            this.indexValue = indexValue;
            this.indexClustering = indexClustering;
            this.timestamp = timestamp;
            this.indexedKey = indexedKey;
            this.indexedEntryClustering = indexedEntryClustering;
            this.indexedEntryCollectionKey = indexedEntryCollectionKey;
        }
    }
}
