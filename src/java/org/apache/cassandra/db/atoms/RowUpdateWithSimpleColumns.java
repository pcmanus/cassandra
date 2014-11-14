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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.FBUtilities;

public class RowUpdateWithSimpleColumns extends AbstractRowUpdate
{
    private final Columns columns;
    private final Cell[] cells;

    private final ColumnDataCursor dataCursor = new ColumnDataCursor();

    private RowUpdateWithSimpleColumns(Columns columns, Cell[] cells)
    {
        this.columns = columns;
        this.cells = cells;
    }

    public RowUpdateWithSimpleColumns(Columns columns)
    {
        this(columns, new Cell[columns.size()]);
    }

    public Columns columns()
    {
        return columns;
    }

    public RowUpdate mergeTo(RowUpdate other, SecondaryIndexManager.Updater indexManager)
    {
        if (other instanceof RowUpdateWithSimpleColumns)
        {
            Columns newColumns = columns.mergeTo(other.columns());
            RowUpdateWithSimpleColumns merged = new RowUpdateWithSimpleColumns(newColumns);
            Rows.merge(clustering(), this, other, FBUtilities.nowInSeconds(), merged.dataCursor, indexManager);
            return merged;
        }

        return other.mergeTo(this, indexManager);
    }

    public boolean isEmpty()
    {
        if (rowTimestamp != Long.MIN_VALUE)
            return false;

        for (int i = 0; i < cells.length; i++)
            if (cells[i] != null)
                return false;
        return true;
    }

    public RowUpdate localCopy(CFMetaData metaData, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public int dataSize()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public long unsharedHeapSizeExcludingData()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public ColumnData data(ColumnDefinition c)
    {
        int idx = columns.indexOf(c);
        return idx < 0 ? null : dataCursor.setTo(idx);
    }

    public Iterator<ColumnData> iterator()
    {
        return dataCursor.reset();
    }

    public RowUpdate updateComplexDeletion(ColumnDefinition c, DeletionTime time)
    {
        throw new UnsupportedOperationException();
    }

    public RowUpdate addCell(ColumnDefinition column, Cell cell)
    {
        int idx = columns.indexOf(column);
        assert idx >= 0;
        cells[idx] = cell.takeAlias();
        return this;
    }

    private class ColumnDataCursor extends UnmodifiableIterator<ColumnData> implements ColumnData, Iterator<ColumnData>, Rows.Writer
    {
        private int currentCol;
        private boolean ready;

        public ColumnDefinition column()
        {
            return columns.get(currentCol);
        }

        public boolean isLive(Cell c, int now)
        {
            return c.localDeletionTime() > now;
        }

        public int size()
        {
            return 1;
        }

        public Cell cell(int i)
        {
            assert i == 0;
            return cells[currentCol];
        }

        public DeletionTime complexDeletionTime()
        {
            return DeletionTime.LIVE;
        }

        public boolean hasNext()
        {
            if (!ready)
            {
                do
                {
                    ++currentCol;
                }
                while (currentCol < columns.size() && cells[currentCol] == null);
                ready = true;
            }
            return currentCol < columns.size();
        }

        public ColumnData next()
        {
            if (!ready && !hasNext())
                throw new NoSuchElementException();

            ready = false;
            return this;
        }

        public ColumnData setTo(int columnIdx)
        {
            currentCol = columnIdx;
            ready = true;
            return this;
        }

        public Iterator<ColumnData> reset()
        {
            currentCol = -1;
            do
            {
                ++currentCol;
            }
            while (currentCol < columns.size() && cells[currentCol] == null);
            ready = true;
            return this;
        }

        public void setClustering(ClusteringPrefix clustering)
        {
            RowUpdateWithSimpleColumns.this.clustering = clustering.takeAlias();
        }

        public void setTimestamp(long rowTimestamp)
        {
            RowUpdateWithSimpleColumns.this.rowTimestamp = rowTimestamp;
        }

        public void newColumn(ColumnDefinition c, DeletionTime complexDeletion)
        {
            assert complexDeletion.isLive();
            currentCol = columns.indexOf(c);
        }

        public void newCell(Cell cell)
        {
            cells[currentCol] = cell.takeAlias();
        }

        public void endOfRow()
        {
        }
    }
}

