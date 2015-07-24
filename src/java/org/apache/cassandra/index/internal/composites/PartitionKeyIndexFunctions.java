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
package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.index.internal.ColumnIndexMetadata;
import org.apache.cassandra.index.internal.ColumnIndexFunctions;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.db.rows.*;
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
public class PartitionKeyIndexFunctions implements ColumnIndexFunctions
{
    public ByteBuffer getIndexedValue(ColumnIndexMetadata metadata,
                                      ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        CompositeType keyComparator = (CompositeType)metadata.baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(partitionKey);
        return components[metadata.indexedColumn.position()];
    }

    public CBuilder buildIndexClusteringPrefix(ColumnIndexMetadata metadata,
                                               ByteBuffer partitionKey,
                                               ClusteringPrefix prefix,
                                               CellPath path)
    {
        CBuilder builder = CBuilder.create(metadata.getIndexComparator());
        builder.add(partitionKey);
        for (int i = 0; i < prefix.size(); i++)
            builder.add(prefix.get(i));
        return builder;
    }

    public IndexEntry decodeEntry(ColumnIndexMetadata metadata,
                                    DecoratedKey indexedValue,
                                    Row indexEntry)
    {
        int ckCount = metadata.baseCfs.metadata.clusteringColumns().size();
        Clustering clustering = indexEntry.clustering();
        CBuilder builder = CBuilder.create(metadata.baseCfs.getComparator());
        for (int i = 0; i < ckCount; i++)
            builder.add(clustering.get(i + 1));

        return new IndexEntry(indexedValue,
                                clustering,
                                indexEntry.primaryKeyLivenessInfo().timestamp(),
                                clustering.get(0),
                                builder.build());
    }

    public boolean isStale(ColumnIndexMetadata metadata,
                           Row data,
                           ByteBuffer indexValue,
                           int nowInSec)
    {
        return !data.hasLiveData(nowInSec);
    }
}
