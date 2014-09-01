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

// TODO: find a better name
public class ReadPartition implements Iterable<Row>
{
    private final CFMetaData metadata;
    private final DecoratedKey key;

    private final Row staticRow;
    private final List<Row> rows = new ArrayList<>();

    private ReadPartition(CFMetaData metadata,
                          DecoratedKey key,
                          Row staticRow)
    {
        this.metadata = metadata;
        this.key = key;
        this.staticRow = staticRow;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public static ReadPartition create(RowIterator iterator)
    {
        assert !iterator.isReverseOrder();

        try (RowIterator iter = iterator)
        {
            ReadPartition p = new ReadPartition(iter.metadata(), iter.partitionKey(), iter.staticRow());
            Iterators.addAll(p.rows, iter);
            return p;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Row getRow(ClusteringPrefix clustering)
    {
        // TODO (binary search)
        throw new UnsupportedOperationException();
    }

    public Iterator<Row> iterator()
    {
        return rows.iterator();
    }

    public RowIterator rowIterator()
    {
        return new RowIterator()
        {
            private int idx;

            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public Row staticRow()
            {
                return staticRow;
            }

            public boolean hasNext()
            {
                return idx < rows.size();
            }

            public Row next()
            {
                return rows.get(idx++);
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

    @Override
    public String toString()
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
