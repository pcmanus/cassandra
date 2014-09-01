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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;

/**
 * Abstract class to make it easier to write iterators that filter some
 * parts of another iterator (used for purging tombstones and removing dropped columns).
 */
public abstract class AbstractFilteringIterator extends WrappingPartitionIterator
{
    private AtomIterator next;

    protected AbstractFilteringIterator(PartitionIterator iter)
    {
        super(iter);
    }

    protected boolean shouldFilter(AtomIterator iterator)
    {
        return true;
    }

    protected boolean shouldFilterPartitionDeletion(DeletionTime dt)
    {
        return false;
    }

    protected boolean shouldFilterRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        return false;
    }

    protected boolean shouldFilterRowTimestamp(long timestamp)
    {
        return false;
    }

    protected boolean shouldFilterComplexDeletionTime(ColumnDefinition c, DeletionTime dt)
    {
        return false;
    }

    protected boolean shouldFilterCell(ColumnDefinition c, Cell cell)
    {
        return false;
    }

    public boolean hasNext()
    {
        while (next == null && super.hasNext())
        {
            next = super.next();
            if (shouldFilter(next))
            {
                next = new AtomFilteringIterator(next);
                if (AtomIterators.isEmpty(next))
                    next = null;
            }

            if (next != null)
                return true;
        }
        return false;
    }

    public AtomIterator next()
    {
        AtomIterator toReturn = next;
        next = null;
        return toReturn;
    }

    private class AtomFilteringIterator extends WrappingAtomIterator
    {
        private final Row staticRow;

        private Atom next;
        private final ReusableRow buffer;
        private final ReusableRow.Writer writer;

        private AtomFilteringIterator(AtomIterator iter)
        {
            super(iter);

            Row r = super.staticRow();
            if (!r.isEmpty())
            {
                ReusableRow tmp = new ReusableRow(iter.staticColumns());
                r = filterRow(r, tmp, tmp.newWriter());
            }
            this.staticRow = r == null ? Rows.EMPTY_STATIC_ROW : r;

            this.buffer = new ReusableRow(iter.columns());
            this.writer = buffer.newWriter();
        }

        public DeletionTime partitionLevelDeletion()
        {
            DeletionTime dt = super.partitionLevelDeletion();
            return shouldFilterPartitionDeletion(dt) ? DeletionTime.LIVE : dt;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public boolean hasNext()
        {
            while (next == null && super.hasNext())
            {
                next = super.next();
                if (next.kind() == Atom.Kind.ROW)
                    next = filterRow((Row)next, buffer, writer);
                else
                    next = filterMarker((RangeTombstoneMarker)next);

                if (next != null)
                    return true;
            }
            return false;
        }

        public Atom next()
        {
            Atom toReturn = next;
            next = null;
            return toReturn;
        }

        public Row filterRow(Row row, ReusableRow buffer, ReusableRow.Writer writer)
        {
            writer.reset();
            writer.setTimestamp(shouldFilterRowTimestamp(row.timestamp()) ? Long.MIN_VALUE : row.timestamp());

            for (ColumnData data : row)
            {
                ColumnDefinition c = data.column();
                DeletionTime dt = data.complexDeletionTime();
                DeletionTime filtered = c.isComplex() && !shouldFilterComplexDeletionTime(c, dt) ? dt : DeletionTime.LIVE;
                writer.newColumn(c, filtered);

                for (int i = 0; i < data.size(); i++)
                {
                    Cell cell = data.cell(i);
                    if (!shouldFilterCell(c, cell))
                        writer.newCell(cell);
                }
            }

            writer.endOfRow();
            return buffer.isEmpty() ? null : buffer;
        }

        public RangeTombstoneMarker filterMarker(RangeTombstoneMarker marker)
        {
            return shouldFilterRangeTombstoneMarker(marker) ? null : marker;
        }
    }
}
