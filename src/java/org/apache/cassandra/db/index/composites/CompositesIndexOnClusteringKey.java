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
public class CompositesIndexOnClusteringKey extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        // Index cell names are rk ck_0 ... ck_{i-1} ck_{i+1} ck_n, so n
        // components total (where n is the number of clustering keys)
        int ckCount = baseMetadata.clusteringColumns().size();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(ckCount);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < columnDef.position(); i++)
            types.add(baseMetadata.clusteringColumns().get(i).type);
        for (int i = columnDef.position() + 1; i < ckCount; i++)
            types.add(baseMetadata.clusteringColumns().get(i).type);
        return new ClusteringComparator(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        return clustering.get(columnDef.position());
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        int count = Math.min(baseCfs.metadata.clusteringColumns().size(), clustering.size());
        CBuilder builder = new CBuilder(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), count); i++)
            builder.add(clustering.get(i));
        for (int i = columnDef.position() + 1; i < count; i++)
            builder.add(clustering.get(i));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexClustering, Cell indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringColumns().size();

        CBuilder builder = new CBuilder(baseCfs.getComparator());
        for (int i = 0; i < columnDef.position(); i++)
            builder.add(indexClustering.get(i + 1));

        builder.add(indexedValue.getKey());

        for (int i = columnDef.position() + 1; i < ckCount; i++)
            builder.add(indexClustering.get(i));

        return new IndexedEntry(indexedValue, indexClustering, indexEntry.timestamp(), indexClustering.get(0), builder.build());
    }

    @Override
    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition c)
    {
        return clustering.get(columnDef.position()) != null;
    }

    public boolean isStale(IndexedEntry entry, Row data, int nowInSec)
    {
        return !Rows.hasLiveData(data, nowInSec);
    }
}
