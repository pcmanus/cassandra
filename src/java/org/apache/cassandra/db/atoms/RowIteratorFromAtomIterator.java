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
package org.apache.cassandra.db.atoms;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

public class RowIteratorFromAtomIterator extends AbstractIterator<Row> implements RowIterator
{
    private final AtomIterator iter;
    private final FilteringRow filter;

    public RowIteratorFromAtomIterator(AtomIterator iter, final int nowInSec)
    {
        this.iter = iter;
        this.filter = new FilteringRow()
        {
            protected boolean includeTimestamp(long timestamp)
            {
                return false;
            }

            protected boolean include(ColumnDefinition column)
            {
                return true;
            }

            protected boolean includeCell(Cell cell)
            {
                return Cells.isLive(cell, nowInSec);
            }

            protected boolean includeDeletion(ColumnDefinition c, DeletionTime dt)
            {
                return false;
            }
        };
    }

    public CFMetaData metadata()
    {
        return iter.metadata();
    }

    public boolean isReverseOrder()
    {
        return iter.isReverseOrder();
    }

    public PartitionColumns columns()
    {
        return iter.columns();
    }

    public DecoratedKey partitionKey()
    {
        return iter.partitionKey();
    }

    public Row staticRow()
    {
        Row row = iter.staticRow();
        return row.isEmpty() ? row : filter.setTo(iter.staticRow());
    }

    protected Row computeNext()
    {
        while (iter.hasNext())
        {
            Atom next = iter.next();
            if (next.kind() != Atom.Kind.ROW)
                continue;

            Row row = filter.setTo((Row)next);
            if (!row.isEmpty())
                return row;
        }
        return endOfData();
    }

    public void close() throws IOException
    {
        iter.close();
    }
}
