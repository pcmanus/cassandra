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
 * Index on a REGULAR column definition on a composite type.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is indexed if c_name == columnDef.name, and it will generate
 * (makeIndexColumnName()) an index entry whose:
 *   - row key will be the value v (getIndexedValue()).
 *   - cell name will
 *       rk ck_0 ... ck_n
 *     where rk is the row key of the initial cell. I.e. the index entry store
 *     all the information require to locate back the indexed cell.
 */
public class CompositesIndexOnRegular extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 1);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(baseMetadata.comparator.subtype(i));
        return new ClusteringComparator(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering,  Cell cell)
    {
        return cell.value();
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        CBuilder builder = new CBuilder(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), clustering.size()); i++)
            builder.add(clustering.get(i));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, ClusteringPrefix indexClustering, Cell indexEntry)
    {
        CBuilder builder = new CBuilder(baseCfs.getComparator());
        for (int i = 0; i < columnDef.position(); i++)
            builder.add(indexClustering.get(i + 1));
        return new IndexedEntry(indexedValue, indexClustering, indexEntry.timestamp(), indexClustering.get(0), builder.build());
    }

    @Override
    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition c)
    {
        return c.name.equals(columnDef.name);
    }

    public boolean isStale(IndexedEntry entry, Row data, int nowInSec)
    {
        Cell cell = Rows.getCell(data, columnDef);
        return cell == null
            || !Cells.isLive(cell, nowInSec)
            || columnDef.type.compare(entry.indexValue.getKey(), cell.value()) != 0;
    }
}
