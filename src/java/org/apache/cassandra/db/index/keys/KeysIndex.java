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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Implements a secondary index for a column family using a second column family.
 * The design uses inverted index http://en.wikipedia.org/wiki/Inverted_index.
 * The row key is the indexed value. For example, if we're indexing a column named
 * city, the index value of city is the row key.
 * The column names are the keys of the records. To see a detailed example, please
 * refer to wikipedia.
 */
public class KeysIndex extends AbstractSimplePerColumnSecondaryIndex
{
    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        return cell.value();
    }

    protected ClusteringPrefix makeIndexClustering(ByteBuffer rowKey, ClusteringPrefix clustering, Cell cell)
    {
        return new SimpleClusteringPrefix(rowKey);
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
    {
        return new KeysSearcher(baseCfs.indexManager, columns);
    }

    // TODO
    //public boolean isIndexEntryStale(ByteBuffer indexedValue, ColumnFamily data, long now)
    //{
    //    Cell cell = data.getColumn(data.getComparator().makeCellName(columnDef.name.bytes));
    //    return cell == null || !cell.isLive(now) || columnDef.type.compare(indexedValue, cell.value()) != 0;
    //}

    public void validateOptions() throws ConfigurationException
    {
        // no options used
    }

    public boolean indexes(ClusteringPrefix clustering, ColumnDefinition column)
    {
        // This consider the full cellName directly
        AbstractType<?> comparator = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return comparator.compare(columnDef.name.bytes, column.name.bytes) == 0;
    }

    //protected AbstractType getExpressionComparator()
    //{
    //    return baseCfs.getComparator().asAbstractType();
    //}
}
