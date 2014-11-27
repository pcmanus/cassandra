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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on a PARTITION_KEY column definition.
 *
 * This suppose a composite row key:
 *   rk = rk_0 ... rk_n
 *
 * The corresponding index entry will be:
 *   - index row key will be rk_i (where i == columnDef.componentIndex)
 *   - cell name will be: rk ck
 *     where rk is the fully partition key and ck the clustering keys of the
 *     original cell names (thus excluding the last column name as we want to refer to
 *     the whole CQL3 row, not just the cell itself)
 *
 * Note that contrarily to other type of index, we repeat the indexed value in
 * the index cell name (we use the whole partition key). The reason is that we
 * want to order the index cell name by partitioner first, and skipping a part
 * of the row key would change the order.
 */
public class CompositesIndexOnPartitionKey extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int ckCount = baseMetadata.clusteringColumns().size();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(ckCount + 1);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < ckCount; i++)
            types.add(baseMetadata.comparator.subtype(i));
        return new ClusteringComparator(types, true, true);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        CompositeType keyComparator = (CompositeType)baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(rowKey);
        return components[columnDef.position()];
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        int count = Math.min(baseCfs.metadata.clusteringColumns().size(), clustering.size());
        CBuilder builder = new CBuilder(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < count; i++)
            builder.add(clustering.get(i));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexClustering, Cell indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringColumns().size();
        CBuilder builder = new CBuilder(baseCfs.getComparator());
        for (int i = 0; i < ckCount; i++)
            builder.add(indexClustering.get(i + 1));

        return new IndexedEntry(indexedValue, indexClustering, indexEntry.timestamp(), indexClustering.get(0), builder.build());
    }

    @Override
    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition c)
    {
        // Since a partition key is always full, we always index it
        return true;
    }

    public boolean isStale(IndexedEntry entry, Row data, int nowInSec)
    {
        return !Rows.hasLiveData(data, nowInSec);
    }
}
