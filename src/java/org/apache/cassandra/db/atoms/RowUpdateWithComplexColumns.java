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
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.FBUtilities;

class RowUpdateWithComplexColumns extends AbstractRowUpdate
{
    private final Columns columns;
    private final ColumnData[] data;

    private RowUpdateWithComplexColumns(Columns columns, ColumnData[] data)
    {
        this.columns = columns;
        this.data = data;
    }

    public RowUpdateWithComplexColumns(Columns columns)
    {
        this(columns, new ColumnData[columns.size()]);
    }

    public Columns columns()
    {
        return columns;
    }

    public RowUpdate mergeTo(RowUpdate other, SecondaryIndexManager.Updater indexUpdater)
    {
        Columns newColumns = columns.mergeTo(other.columns());
        RowUpdateWithComplexColumns merged = new RowUpdateWithComplexColumns(newColumns);
        Rows.merge(clustering(), this, other, FBUtilities.nowInSeconds(), merged.new Writer(), indexUpdater);
        return merged;
    }

    public boolean isEmpty()
    {
        if (rowTimestamp != Long.MIN_VALUE)
            return false;

        for (int i = 0; i < data.length; i++)
            if (data[i] != null)
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
        return idx < 0 ? null : data[idx];
    }

    public Iterator<ColumnData> iterator()
    {
        return Iterators.forArray(data);
    }

    public RowUpdate updateComplexDeletion(ColumnDefinition c, DeletionTime time)
    {
        if (!c.isComplex())
            return this;

        int idx = columns.indexOf(c);
        assert idx >= 0;
        updateComplexDeletion(c, idx, time);
        return this;
    }

    private void updateComplexDeletion(ColumnDefinition c, int columnIdx, DeletionTime time)
    {
        ColumnData d = data[columnIdx];
        ComplexColumnData cd;
        if (d == null)
        {
            cd = new ComplexColumnData(c);
            data[columnIdx] = cd;
        }
        else
        {
            assert d instanceof ComplexColumnData;
            cd = (ComplexColumnData)d;
        }
        cd.setDeletion(time);
    }

    public RowUpdate addCell(ColumnDefinition column, Cell cell)
    {
        int idx = columns.indexOf(column);
        assert idx >= 0;
        addCell(column, idx, cell);
        return this;
    }

    private void addCell(ColumnDefinition column, int columnIdx, Cell cell)
    {
        if (column.isComplex())
        {
            ColumnData d = data[columnIdx];
            if (d == null)
            {
                ComplexColumnData cd = new ComplexColumnData(column);
                cd.append(cell);
                data[columnIdx] = cd;
            }
            else
            {
                assert d instanceof ComplexColumnData;
                ((ComplexColumnData)d).append(cell);
            }
        }
        else
        {
            data[columnIdx] = new SimpleColumnData(column, cell);
        }
    }

    private class Writer implements Rows.Writer
    {
        private int columnIdx;

        public void setClustering(ClusteringPrefix clustering)
        {
            RowUpdateWithComplexColumns.this.clustering = clustering.takeAlias();
        }

        public void setTimestamp(long rowTimestamp)
        {
            RowUpdateWithComplexColumns.this.rowTimestamp = rowTimestamp;
        }

        public void newColumn(ColumnDefinition c, DeletionTime complexDeletion)
        {
            columnIdx = columns.indexOf(c);

            if (!complexDeletion.isLive())
                updateComplexDeletion(c, columnIdx, complexDeletion);
        }

        public void newCell(Cell cell)
        {
            addCell(columns.get(columnIdx), columnIdx, cell);
        }

        public void endOfRow()
        {
        }
    }

    private static class SimpleColumnData implements ColumnData
    {
        private final ColumnDefinition column;
        private final Cell cell;

        private SimpleColumnData(ColumnDefinition column, Cell cell)
        {
            this.column = column;
            this.cell = cell;
        }

        public ColumnDefinition column()
        {
            return column;
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
            return cell;
        }

        public DeletionTime complexDeletionTime()
        {
            return DeletionTime.LIVE;
        }
    }

    // TODO: This is not terribly optimized, we could probably do better, though we might be able to
    // to better for the whole RowUpdate anyway so keeping things reasonably simple for now.
    private static class ComplexColumnData implements ColumnData
    {
        private static final int INITIAL_CAPACITY = 5;

        private final ColumnDefinition column;
        private DeletionTime complexDeletion;
        private Cell[] cells;
        private int size;

        private ComplexColumnData(ColumnDefinition column)
        {
            this.column = column;
            this.cells = new Cell[INITIAL_CAPACITY];
        }

        private void setDeletion(DeletionTime delTime)
        {
            this.complexDeletion = delTime.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isLive(Cell c, int now)
        {
            return c.localDeletionTime() > now && complexDeletion.markedForDeleteAt() < c.timestamp();
        }

        public int size()
        {
            return size;
        }

        public Cell cell(int i)
        {
            return cells[i];
        }

        public void append(Cell cell)
        {
            ensureCapacity(size + 1);
            cells[size++] = cell.takeAlias();
        }

        public void add(Cell cell)
        {
            if (size == 0 || column.cellComparator().compare(cells[size - 1], cell) < 0)
            {
                append(cell);
                return;
            }

            int idx = Arrays.binarySearch(cells, 0, size, column.cellComparator());
            if (idx >= 0)
            {
                cells[idx] = cell.takeAlias();
            }
            else
            {
                ensureCapacity(size + 1);
                idx = -idx - 1;
                System.arraycopy(cells, idx, cells, idx+1, size - idx);
                cells[idx] = cell.takeAlias();
                ++size;
            }
        }

        private void ensureCapacity(int capacity)
        {
            if (cells.length >= capacity)
                return;

            int newCapacity = (capacity * 3) / 2 + 1;
            cells = Arrays.copyOf(cells, newCapacity);
        }

        public DeletionTime complexDeletionTime()
        {
            return complexDeletion;
        }
    }
}

