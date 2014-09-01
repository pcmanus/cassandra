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
 * Index the value of a collection cell.
 *
 * This is a lot like an index on REGULAR, except that we also need to make
 * the collection key part of the index entry so that:
 *   1) we don't have to scan the whole collection at query time to know the
 *   entry is stale and if it still satisfies the query.
 *   2) if a collection has multiple time the same value, we need one entry
 *   for each so that if we delete one of the value only we only delete the
 *   entry corresponding to that value.
 */
public class CompositesIndexOnCollectionValue extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<>(prefixSize + 2);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(baseMetadata.comparator.subtype(i));
        types.add(((CollectionType)columnDef.type).nameComparator()); // collection key
        return new ClusteringComparator(types);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).valueComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        return cell.value();
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        CBuilder builder = new CBuilder(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), clustering.size()); i++)
            builder.add(clustering.get(i));

        // When indexing, cell will be present, but when searching, it won't  (CASSANDRA-7525)
        if (cell != null)
            builder.add(((CollectionPath)cell.path()).element());
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexClustering, Cell indexEntry)
    {
        int prefixSize = columnDef.position();
        CBuilder builder = new CBuilder(baseCfs.getComparator());
        for (int i = 0; i < prefixSize; i++)
            builder.add(indexClustering.get(i + 1));
        return new IndexedEntry(indexedValue, indexClustering, indexEntry.timestamp(), indexClustering.get(0), builder.build(), indexClustering.get(prefixSize + 1));
    }

    @Override
    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition column)
    {
        return column.name.equals(columnDef.name);
    }

    public boolean isStale(IndexedEntry entry, Row data, int nowInSec)
    {
        Cell cell = Rows.getCell(data, columnDef, new CollectionPath(entry.indexedEntryCollectionKey));
        return cell == null
            || !Cells.isLive(cell, nowInSec)
            || ((CollectionType) columnDef.type).valueComparator().compare(entry.indexValue.getKey(), cell.value()) != 0;
    }
}
