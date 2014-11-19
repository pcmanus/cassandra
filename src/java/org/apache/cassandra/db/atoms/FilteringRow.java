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

import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

public abstract class FilteringRow implements Row
{
    private Row wrapped;
    private final ReusableCellIterator cellIterator = new ReusableCellIterator()
    {
        protected Cell computeNext()
        {
            while (wrapped.hasNext())
            {
                Cell next = wrapped.next();
                if (include(next.column()) && includeCell(next))
                    return next;
            }
            return endOfData();
        }
    };

    public FilteringRow setTo(Row row)
    {
        this.wrapped = row;
    }

    protected abstract boolean includeTimestamp(long timestamp);
    protected abstract boolean include(ColumnDefinition column);

    protected abstract boolean includeCell(Cell cell);
    protected abstract boolean includeDeletion(ColumnDefinition c, DeletionTime dt);

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    public ClusteringPrefix clustering()
    {
        return wrapped.clustering();
    }

    public Columns columns()
    {
        return wrapped.columns();
    }

    public long timestamp()
    {
        long timestamp = wrapped.timestamp();
        return includeTimestamp(timestamp) ? timestamp : Rows.NO_TIMESTAMP;
    }

    public boolean isEmpty()
    {
        if (timestamp() != Long.MIN_VALUE || iterator().hasNext())
            return false;

        // We also need to verify that none of the complex columns has any deletion info
        for (int i = 0; i < columns().complexColumnCount(); i++)
            if (!getDeletion(columns().getComplex(i)).isLive())
                return false;

        return true;
    }

    public Cell getCell(ColumnDefinition c)
    {
        if (!include(c))
            return null;

        Cell cell = wrapped.getCell(c);
        return includeCell(cell) ? cell : null;
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        if (!include(c))
            return null;

        Iterator<Cell> cells = wrapped.getCells(c);
        return cells == null ? null : cellIterator.setTo(cells);
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        if (!include(c))
            return DeletionTime.LIVE;

        DeletionTime dt = wrapped.getDeletion(c);
        return includeDeletion(c, dt) ? dt : DeletionTime.LIVE;
    }

    public Iterator<Cell> iterator()
    {
        return cellIterator.setTo(wrapped.iterator());
    }

    public Row takeAlias()
    {
        return Rows.copy(this);
    }
}
