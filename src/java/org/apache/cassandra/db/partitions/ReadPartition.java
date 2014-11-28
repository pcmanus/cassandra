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
package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.SearchIterator;

// TODO: find a better name
public class ReadPartition extends ArrayBackedPartition
{
    private ReadPartition(CFMetaData metadata,
                          DecoratedKey partitionKey,
                          PartitionColumns columns,
                          int initialRowCapacity)
    {
        super(metadata, partitionKey, DeletionTime.LIVE, columns, initialRowCapacity);
    }

    public static ReadPartition create(RowIterator iterator)
    {
        assert !iterator.isReverseOrder();

        ReadPartition partition = new ReadPartition(iterator.metadata(),
                                                    iterator.partitionKey(),
                                                    iterator.columns(),
                                                    4);

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer();

        try (RowIterator iter = iterator)
        {
            while (iter.hasNext())
                Rows.copy(iter.next(), writer);
        }
        return partition;
    }

    public RowIterator rowIterator()
    {
        final Iterator<Row> iter = iterator();
        return new RowIterator()
        {
            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public PartitionColumns columns()
            {
                return columns;
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public Row staticRow()
            {
                return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            }

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next()
            {
                return iter.next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
            }
        };

    }

    public Row getRow(ClusteringPrefix clustering)
    {
        return searchIterator().next(clustering);
    }

    @Override
    public String toString()
    {
        return RowIterators.toString(rowIterator());
    }
}
