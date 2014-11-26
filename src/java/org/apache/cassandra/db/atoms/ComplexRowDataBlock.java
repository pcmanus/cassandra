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

/**
 * Contains complex cells data for one or more rows.
 */
public class ComplexRowDataBlock
{
    private final Columns columns;

    /*
     * For a given complex column c, we have to store an unknown number of
     * cells. So for each column of each row, we keep pointers (in data)
     * to the start and end of the cells for this column (cells for a given
     * columns are thus stored contiguously).
     * For instance, if columns has 'c' complex columns, the x-th column of
     * row 'n' will have it's cells in data at indexes
     *    [cellIdx[(n * c) + x], cellIdx[(n * c) + x + 1])
     */
    private int[] cellIdx;
    final CellData data;

    // Complex cells has also a path. Those are indexed like the cells in
    // data, through cellIdx.
    private CellPath[] complexPaths;

    // For each complex column, it's deletion time (if any).
    DeletionTimeArray complexDelTimes;

    public ComplexRowDataBlock(Columns columns, int rows)
    {
        this.columns = columns;

        // We start with an estimated 4 cells per complex column. The arrays
        // will grow if needed so this is just a somewhat random estimation.
        int cellCount = rows * columns.complexColumnCount() * 4;
        this.data = new CellData(cellCount);
        this.complexPaths = new CellPath[cellCount];
    }

    public Columns columns()
    {
        return columns;
    }

    // Swap row i and j
    public void swap(int i, int j)
    {
        throw new UnsupportedOperationException();
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        throw new UnsupportedOperationException();
    }

    // Move row i into j
    public void move(int i, int j)
    {
        throw new UnsupportedOperationException();
    }

    public CellWriter cellWriter()
    {
        return new CellWriter();
    }

    public int complexDeletionIdx(int row, ColumnDefinition column)
    {
        return (row * columns.complexColumnCount()) + columns.complexIdx(column);
    }

    public boolean hasComplexDeletion(int row)
    {
        int base = row * columns.complexColumnCount();
        for (int i = base; i < base + columns.complexColumnCount(); i++)
            if (!complexDelTimes.isLive(i))
                return true;
        return false;
    }

    public static ReusableIterator reusableComplexCells()
    {
        return new ReusableIterator();
    }

    public static DeletionTimeArray.Cursor complexDeletionCursor()
    {
        return new DeletionTimeArray.Cursor();
    }

    public static ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    private void ensureComplexPathsCapacity(int idxToSet)
    {
        int capacity = complexPaths.length;
        if (idxToSet < capacity)
            return;

        int newCapacity = (capacity * 3) / 2 + 1;
        complexPaths = Arrays.copyOf(complexPaths, newCapacity);
    }

    private static class ReusableCell extends CellData.ReusableCell
    {
        private ComplexRowDataBlock dataBlock;

        ReusableCell setTo(ComplexRowDataBlock dataBlock, ColumnDefinition column, int idx)
        {
            this.dataBlock = dataBlock;
            super.setTo(dataBlock.data, column, idx);
            return this;
        }

        @Override
        public CellPath path()
        {
            return dataBlock.complexPaths[idx];
        }
    }

    static class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private ComplexRowDataBlock dataBlock;
        private final ReusableCell cell = new ReusableCell();
        private int idx;
        private int endIdx;

        private ReusableIterator()
        {
        }

        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row, ColumnDefinition column)
        {
            this.dataBlock = dataBlock;
            if (dataBlock == null)
                return null;

            int columnIdx = 2 * ((row * dataBlock.columns.complexColumnCount()) + dataBlock.columns.complexIdx(column));
            idx = dataBlock.cellIdx[columnIdx];
            endIdx = dataBlock.cellIdx[columnIdx + 1];

            return endIdx <= idx ? null : this;
        }

        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row)
        {
            this.dataBlock = dataBlock;
            if (dataBlock == null)
                return null;

            int columnCount = dataBlock.columns.complexColumnCount();
            int columnIdx = 2 * row * columnCount;

            // find the index of the first cell of the row
            for (int i = columnIdx; i < columnIdx + (2 * columnCount); i += 2)
            {
                if (dataBlock.cellIdx[i + 1] > dataBlock.cellIdx[i])
                {
                    idx = dataBlock.cellIdx[i];
                    break;
                }
            }

            // find the index of the last cell of the row
            for (int i = columnIdx + (2 * columnCount) - 1; i >= columnIdx; i -= 2)
            {
                if (dataBlock.cellIdx[i + 1] > dataBlock.cellIdx[i])
                {
                    endIdx = dataBlock.cellIdx[i + 1];
                    break;
                }
            }

            return endIdx <= idx ? null : this;
        }

        public boolean hasNext()
        {
            if (dataBlock == null)
                return false;

            return idx < endIdx;
        }

        public Cell next()
        {
            cell.setTo(dataBlock, dataBlock.columns.getComplex(idx), idx);
            ++idx;
            return cell;
        }
    }

    public class CellWriter
    {
        private int base;

        // Index of the next free slot in data.
        private int idx;

        public void addCell(ColumnDefinition column, ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            int columnIdx = base + columns.complexIdx(column);

            int start = cellIdx[columnIdx];
            int end = cellIdx[columnIdx + 1];
            if (end <= start)
            {
                // First cell for the complex column
                cellIdx[columnIdx] = idx;
                cellIdx[columnIdx + 1] = idx + 1;
            }
            else
            {
                cellIdx[columnIdx + 1] = idx + 1;
            }

            data.setCell(idx, value, timestamp, localDeletionTime, ttl);
            ensureComplexPathsCapacity(idx);
            complexPaths[idx] = path;
            ++idx;
        }

        public void setComplexDeletion(ColumnDefinition column, DeletionTime deletionTime)
        {
            int columnIdx = base + columns.complexIdx(column);
            complexDelTimes.set(columnIdx, deletionTime);
        }

        public void endOfRow()
        {
            base += columns.complexColumnCount();
        }

        public void reset()
        {
            base = 0;
            idx = 0;
        }
    }
}
