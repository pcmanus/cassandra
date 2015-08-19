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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.index.internal.ColumnIndexMetadata;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on the element and value of cells participating in a collection.
 *
 * The row keys for this index are a composite of the collection element
 * and value of indexed columns.
 */
public class CollectionEntryIndexFunctions extends CollectionKeyIndexFunctionsBase
{
    public AbstractType<?> getIndexValueType(ColumnDefinition indexedColumn)
    {
        CollectionType colType = (CollectionType)indexedColumn.type;
        return CompositeType.getInstance(colType.nameComparator(), colType.valueComparator());
    }

    public ByteBuffer getIndexedValue(ColumnIndexMetadata metadata,
                                      ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return CompositeType.build(path.get(0), cellValue);
    }

    public boolean isStale(ColumnIndexMetadata metadata,
                           Row data,
                           ByteBuffer indexValue,
                           int nowInSec)
    {
        ByteBuffer[] components = ((CompositeType)getIndexValueType(metadata.indexedColumn)).split(indexValue);
        ByteBuffer mapKey = components[0];
        ByteBuffer mapValue = components[1];

        ColumnDefinition columnDef = metadata.indexedColumn;
        Cell cell = data.getCell(columnDef, CellPath.create(mapKey));
        if (cell == null || !cell.isLive(nowInSec))
            return true;

        AbstractType<?> valueComparator = ((CollectionType)columnDef.type).valueComparator();
        return valueComparator.compare(mapValue, cell.value()) != 0;
    }
}
