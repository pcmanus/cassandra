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

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

public class ColumnDataContainer
{
    // columns stored
    private final Columns columns;
    // Number of complex columns
    private final int complexColumns;

    // Cells start and end positions for each column of each row.
    // Note: we record both start and end positions which allows to support inserting cells
    // in unsorted order (for a given row, useful for the write path). It also simplify the
    // implementation a bit. In the cases where cells are added in sorted order anyway (the
    // read path), it would be possible to use only one array for the start position and the
    // end position would be computed by used the next non-negative position in the array. We
    // keep it simple for now though.
    private int[] columnStartPositions;
    private int[] columnEndPositions;

    private ByteBuffer[] values;
    private long[] timestamps;
    private int[] localDelTimes;
    private int[] ttls;

    private CellPath[] complexPaths;
    private DeletionTimeArray complexDelTimes;

    public ColumnDataContainer(Columns columns, int rowsCapacity, int cellsCapacity)
    {
        this.columns = columns;

        this.columnStartPositions = new int[rowsCapacity * columns.size()];
        this.columnEndPositions = new int[rowsCapacity * columns.size()];

        this.values = new ByteBuffer[cellsCapacity];
        this.timestamps = new long[cellsCapacity];
        this.localDelTimes = new int[cellsCapacity];
        this.ttls = new int[cellsCapacity];

        int complex = 0;
        for (int i = 0; i < columns.size(); i++)
            if (columns.get(i).isComplex())
                complex++;

        this.complexColumns = complex;
        this.complexPaths = complex > 0 ? new CellPath[cellsCapacity] : null;
        this.complexDelTimes = complex > 0 ? new DeletionTimeArray(rowsCapacity * complex) : null;
    }

    public Columns columns()
    {
        return columns;
    }

    private int columnBase(int rowOffset)
    {
        return rowOffset * columns.size();
    }

    private int complexBase(int rowOffset)
    {
        return rowOffset * complexColumns;
    }

    private int cellsCount(int rowOffset)
    {
        int base = rowOffset * columns.size();

        int first = -1;
        for (int i = 0; i < columns.size(); i++)
        {
            int idx = base + i;
            if (columnStartPositions[idx] < columnEndPositions[idx])
            {
                first = columnStartPositions[idx];
                break;
            }
        }

        if (first == -1)
            return 0;

        int last = -1;
        for (int i = columns.size() - 1; i >= 0; i--)
        {
            int idx = base + i;
            if (columnStartPositions[idx] < columnEndPositions[idx])
            {
                last = columnEndPositions[idx];
                break;
            }
        }

        assert last != -1;
        return last - first;
    }

    private int columnIdx(ColumnDefinition c, int prevIdx)
    {
        for (int i = prevIdx + 1; i < columns.size(); i++)
            if (columns.get(i) == c)
                return i;
        throw new AssertionError();
    }

    private int complexIdx(ColumnDefinition c)
    {
        int idx = 0;
        for (int i = 0; i < columns.size(); i++)
        {
            if (!columns.get(i).isComplex())
                continue;

            if (columns.get(i) == c)
                return idx;

            ++idx;
        }
        throw new AssertionError();
    }

    private void setCell(int pos, int columnIdx, Cell cell)
    {
        ensureCapacityForCell(pos);

        values[pos] = cell.value();
        ttls[pos] = cell.ttl();
        timestamps[pos] = cell.timestamp();
        localDelTimes[pos] = cell.localDeletionTime();

        if (columns.get(columnIdx).isComplex())
            complexPaths[pos] = cell.path();
    }

    private void ensureCapacityForRow(int rowToSet)
    {
        int currentCapacity = columnStartPositions.length / columns.size();
        if (rowToSet < currentCapacity)
            return;

        int newCapacity = (currentCapacity * 3) / 2 + 1;
        columnStartPositions = Arrays.copyOf(columnStartPositions, newCapacity * columns.size());
        columnEndPositions = Arrays.copyOf(columnEndPositions, newCapacity * columns.size());

        if (complexColumns > 0)
            complexDelTimes.resize(newCapacity * complexColumns);
    }

    private void ensureCapacityForCell(int idxToSet)
    {
        if (idxToSet < values.length)
            return;

        int newSize = (values.length * 3) / 2 + 1;

        values = Arrays.copyOf(values, newSize);
        timestamps = Arrays.copyOf(timestamps, newSize);
        localDelTimes = Arrays.copyOf(localDelTimes, newSize);
        ttls = Arrays.copyOf(ttls, newSize);
        if (complexPaths != null)
            complexPaths = Arrays.copyOf(complexPaths, newSize);
    }

    public void clear()
    {
        // Note that it's enough to clear columnStartPositions/columnEndPositions
        // and complexDelTimes.
        // The other arrays will be overwritten during write and we won't read anything
        // we shouldn't as long as those other arrays are correct.
        Arrays.fill(columnStartPositions, 0);
        Arrays.fill(columnEndPositions, 0);
        complexDelTimes.clear();
    }

    public static abstract class Reader implements Row
    {
        private final ColumnDataContainer data;
        private final ReusableColumnData columnData = new ReusableColumnData();

        private int columnIdx = -1;
        private int position;

        protected Reader(ColumnDataContainer data)
        {
            this.data = data;
        }

        protected ColumnDataContainer data()
        {
            return data;
        }

        protected abstract int rowOffset();

        private int columnBase()
        {
            return data.columnBase(rowOffset());
        }

        private int complexBase()
        {
            return data.complexBase(rowOffset());
        }

        protected int cellsCount()
        {
            return data.cellsCount(rowOffset());
        }

        public Atom.Kind kind()
        {
            return Atom.Kind.ROW;
        }

        public boolean isEmpty()
        {
            return timestamp() == Long.MIN_VALUE && cellsCount() == 0;
        }

        public ColumnData data(ColumnDefinition c)
        {
            return columnData.setTo(c);
        }

        public Iterator<ColumnData> iterator()
        {
            return columnData.reset();
        }

        public void reset()
        {
            columnIdx = -1;
        }

        public class ReusableColumnData extends UnmodifiableIterator<ColumnData> implements ColumnData
        {
            private final ReusableCell cell = new ReusableCell();
            private final DeletionTimeArray.Cursor complexDeletion = data.complexDelTimes == null
                                                                   ? null
                                                                   : data.complexDelTimes.newCursor();

            private boolean ready;

            public ColumnData setTo(ColumnDefinition c)
            {
                columnIdx = data.columnIdx(c, -1);
                position = data.columnStartPositions[columnBase() + columnIdx];
                ready = true;
                return this;
            }

            public ColumnDefinition column()
            {
                return data.columns.get(columnIdx);
            }

            public DeletionTime complexDeletionTime()
            {
                return column().isComplex()
                     ? complexDeletion.setTo(complexBase() + data.complexIdx(column()))
                     : DeletionTime.LIVE;
            }

            public boolean isLive(Cell c, int nowInSec)
            {
                return c.localDeletionTime() > nowInSec
                    && (data.complexDelTimes == null || c.timestamp() > complexDeletionTime().markedForDeleteAt());
            }

            public int size()
            {
                int idx = columnBase() + columnIdx;
                int start = data.columnStartPositions[idx];
                int end = data.columnEndPositions[idx];
                return start < end ? end - start : 0;
            }

            public Cell cell(int i)
            {
                return cell.setTo(position + i);
            }

            public boolean hasNext()
            {
                if (!ready)
                {
                    ++columnIdx;

                    // Skip columns that have no data (no cells and either not a complex column or one with no deletion),
                    int idx = columnBase() + columnIdx;
                    while (idx < data.columnStartPositions.length
                            && data.columnStartPositions[idx] >= data.columnEndPositions[idx]
                            && complexDeletionTime().isLive())
                    {
                        ++columnIdx;
                        idx = columnBase() + columnIdx;
                    }

                    ready = true;
                }

                return columnIdx < data.columns.size();
            }

            public ColumnData next()
            {
                if (!ready)
                    hasNext();

                position = data.columnStartPositions[columnBase() + columnIdx];
                ready = false;
                return this;
            }

            public Iterator<ColumnData> reset()
            {
                Reader.this.reset();
                ready = false;
                return this;
            }
        }

        public class ReusableCell implements Cell
        {
            private int position = -1;

            public ReusableCell setTo(int position)
            {
                this.position = position;
                return this;
            }

            public boolean isCounterCell()
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            public ByteBuffer value()
            {
                return data.values[position];
            }

            public long timestamp()
            {
                return data.timestamps[position];
            }

            public int ttl()
            {
                return data.ttls[position];
            }

            public int localDeletionTime()
            {
                return data.localDelTimes[position];
            }

            public CellPath path()
            {
                return data.complexPaths == null ? null : data.complexPaths[position];
            }

            public Cell takeAlias()
            {
                // TODO
                throw new UnsupportedOperationException();
            }
        }
    }

    public static abstract class Writer implements Rows.Writer
    {
        private final ColumnDataContainer data;

        private int columnIdx;
        private int position;

        protected Writer(ColumnDataContainer data)
        {
            this.data = data;
            reset();
        }

        protected abstract int startPosition();
        protected abstract int rowOffset();

        private int columnBase()
        {
            return data.columnBase(rowOffset());
        }

        public void newColumn(ColumnDefinition c, DeletionTime complexDeletion)
        {
            if (columnIdx >= 0)
                data.columnEndPositions[columnIdx] = position;

            columnIdx = data.columnIdx(c, columnIdx);
            data.columnStartPositions[columnBase() + columnIdx] = position;

            if (c.isComplex())
                data.complexDelTimes.set(data.complexIdx(c), complexDeletion);
        }

        public void newCell(Cell cell)
        {
            data.setCell(position++, columnIdx, cell);
        }

        public void endOfRow()
        {
            data.columnEndPositions[columnBase() + columnIdx] = position;
        }

        public void reset()
        {
            columnIdx = -1;
            position = startPosition();
        }
    }
}
