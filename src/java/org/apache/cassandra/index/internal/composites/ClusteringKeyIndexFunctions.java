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
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.internal.*;

/**
 * Index on a CLUSTERING_COLUMN column definition.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is always indexed by this index (or rather, it is indexed if
 * n >= columnDef.componentIndex, which will always be the case in practice)
 * and it will generate (makeIndexColumnName()) an index entry whose:
 *   - row key will be ck_i (getIndexedValue()) where i == columnDef.componentIndex.
 *   - cell name will
 *       rk ck_0 ... ck_{i-1} ck_{i+1} ck_n
 *     where rk is the row key of the initial cell and i == columnDef.componentIndex.
 */
public class ClusteringKeyIndexFunctions implements ColumnIndexFunctions
{
    public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                        CFMetaData baseMetadata,
                                                        ColumnDefinition columnDef)
    {
        List<ColumnDefinition> cks = baseMetadata.clusteringColumns();
        for (int i = 0; i < columnDef.position(); i++)
        {
            ColumnDefinition def = cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
        }
        for (int i = columnDef.position() + 1; i < cks.size(); i++)
        {
            ColumnDefinition def = cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
        }
        return builder;
    }

    public ByteBuffer getIndexedValue(ColumnIndexMetadata metadata,
                                      ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return clustering.get(metadata.indexedColumn.position());
    }

    public CBuilder buildIndexClusteringPrefix(ColumnIndexMetadata metadata,
                                               ByteBuffer partitionKey,
                                               ClusteringPrefix prefix,
                                               CellPath path)
    {
        CBuilder builder = CBuilder.create(metadata.getIndexComparator());
        builder.add(partitionKey);
        for (int i = 0; i < Math.min(metadata.indexedColumn.position(), prefix.size()); i++)
            builder.add(prefix.get(i));
        for (int i = metadata.indexedColumn.position() + 1; i < prefix.size(); i++)
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
        for (int i = 0; i < metadata.indexedColumn.position(); i++)
            builder.add(clustering.get(i + 1));

        builder.add(indexedValue.getKey());

        for (int i = metadata.indexedColumn.position() + 1; i < ckCount; i++)
            builder.add(clustering.get(i));

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
