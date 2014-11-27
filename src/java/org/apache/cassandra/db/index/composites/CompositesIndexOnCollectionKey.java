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
 * Index on the collection element of the cell name of a collection.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name [col_elt] : v
 * where ck_i are the cluster keys, c_name the CQL3 column name, col_elt the
 * collection element that we want to index (which may or may not be there depending
 * on whether c_name is the collection we're indexing) and v the cell value.
 *
 * Such a cell is indexed if c_name is the indexed collection (in which case we are guaranteed to have
 * col_elt). The index entry will be:
 *   - row key will be col_elt value (getIndexedValue()).
 *   - cell name will be 'rk ck_0 ... ck_n' where rk is the row key of the initial cell.
 */
public class CompositesIndexOnCollectionKey extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int count = 1 + baseMetadata.clusteringColumns().size(); // row key + clustering prefix
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(count);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < count - 1; i++)
            types.add(baseMetadata.comparator.subtype(i));
        return new ClusteringComparator(types, true, true);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).nameComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        return clustering.get(columnDef.position() + 1);
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        int count = 1 + baseCfs.metadata.clusteringColumns().size();
        CBuilder builder = new CBuilder(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < count - 1; i++)
            builder.add(clustering.get(i));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexClustering, Cell indexEntry)
    {
        int count = 1 + baseCfs.metadata.clusteringColumns().size();
        CBuilder builder = new CBuilder(baseCfs.getComparator());
        for (int i = 0; i < count - 1; i++)
            builder.add(indexClustering.get(i + 1));
        return new IndexedEntry(indexedValue, indexClustering, indexEntry.timestamp(), indexClustering.get(0), builder.build());
    }

    @Override
    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition column)
    {
        return column.name.equals(columnDef.name);
    }

    public boolean isStale(IndexedEntry entry, Row data, int nowInSec)
    {
        Cell cell = Rows.getCell(data, columnDef);
        return cell == null || !Cells.isLive(cell, nowInSec);
    }
}
