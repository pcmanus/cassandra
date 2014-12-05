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
import java.util.*;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Contains complex cells data for one or more rows.
 */
public abstract class ComplexRowDataBlock
{
    private final Columns columns;

    // For each complex column, it's deletion time (if any).
    final DeletionTimeArray complexDelTimes;

    protected ComplexRowDataBlock(Columns columns, int rows)
    {
        this.columns = columns;

        int columnCount = rows * columns.complexColumnCount();
        this.complexDelTimes = new DeletionTimeArray(columnCount);
    }

    public static ComplexRowDataBlock create(Columns columns, int rows, boolean sortable)
    {
        return sortable
             ? new SortableComplexRowDataBlock(columns, rows)
             : new SimpleComplexRowDataBlock(columns, rows);
    }

    public Columns columns()
    {
        return columns;
    }

    protected abstract ComplexCellBlock cellBlock(int row);
    protected abstract ComplexCellBlock cellBlockForWritting(int row);
    protected abstract int cellBlockBase(int row);

    protected abstract void swapCells(int i, int j);
    protected abstract void mergeCells(int i, int j, int nowInSec);
    protected abstract void moveCells(int i, int j);

    protected abstract long cellDataUnsharedHeapSizeExcludingData();
    protected abstract int dataCellSize();
    protected abstract void clearCellData();

    // Swap row i and j
    public void swap(int i, int j)
    {
        swapCells(i, j);

        int s = columns.complexColumnCount();
        for (int k = 0; k < s; k++)
            complexDelTimes.swap(i * s + k, j * s + k);
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        mergeCells(i, j, nowInSec);

        int s = columns.complexColumnCount();
        for (int k = 0; k < s; k++)
            if (complexDelTimes.supersedes(j * s + k, i * s + k))
                complexDelTimes.move(j * s + k, i * s + k);
    }

    // Move row i into j
    public void move(int i, int j)
    {
        moveCells(i, j);
        ensureDelTimesCapacity(Math.max(i, j));
        int s = columns.complexColumnCount();
        for (int k = 0; k < s; k++)
            complexDelTimes.move(i * s + k, j * s + k);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return cellDataUnsharedHeapSizeExcludingData() + complexDelTimes.unsharedHeapSize();
    }

    public int dataSize()
    {
        return dataCellSize() + complexDelTimes.dataSize();
    }

    public CellWriter cellWriter()
    {
        return new CellWriter();
    }

    public int complexDeletionIdx(int row, ColumnDefinition column)
    {
        int baseIdx = columns.complexIdx(column);
        if (baseIdx < 0)
            return -1;

        int idx = (row * columns.complexColumnCount()) + baseIdx;
        return idx < complexDelTimes.size() ? idx : -1;
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

    private void ensureDelTimesCapacity(int rowToSet)
    {
        int originalCapacity = complexDelTimes.size() / columns.complexColumnCount();
        if (rowToSet < originalCapacity)
            return;

        int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);
        complexDelTimes.resize(newCapacity * columns.complexColumnCount());
    }

    private static class SimpleComplexRowDataBlock extends ComplexRowDataBlock
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleComplexRowDataBlock(Columns.NONE, 0));

        private final ComplexCellBlock cells;

        private SimpleComplexRowDataBlock(Columns columns, int rows)
        {
            super(columns, rows);
            this.cells = new ComplexCellBlock(columns, rows);
        }

        protected ComplexCellBlock cellBlock(int row)
        {
            return cells;
        }

        protected ComplexCellBlock cellBlockForWritting(int row)
        {
            cells.ensureCapacity(row);
            return cells;
        }

        protected int cellBlockBase(int row)
        {
            return 2 * row * columns().complexColumnCount();
        }

        // Swap cells from row i and j
        public void swapCells(int i, int j)
        {
            throw new UnsupportedOperationException();
        }

        // Merge cells from row i into j
        public void mergeCells(int i, int j, int nowInSec)
        {
            throw new UnsupportedOperationException();
        }

        // Move cells from row i into j
        public void moveCells(int i, int j)
        {
            throw new UnsupportedOperationException();
        }

        protected long cellDataUnsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + cells.unsharedHeapSizeExcludingData();
        }

        protected int dataCellSize()
        {
            return cells.dataSize();
        }

        protected void clearCellData()
        {
            cells.clear();
        }
    }

    private static class SortableComplexRowDataBlock extends ComplexRowDataBlock
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SortableComplexRowDataBlock(Columns.NONE, 0));

        private final List<ComplexCellBlock> cells;

        private SortableComplexRowDataBlock(Columns columns, int rows)
        {
            super(columns, rows);
            this.cells = new ArrayList<>(rows);
        }

        protected ComplexCellBlock cellBlockForWritting(int row)
        {
            if (row < cells.size())
                return cells.get(row);

            // Some rows may have had no complex cells at all, so use nulls for them
            ensureCapacity(row-1);

            assert row == cells.size();
            ComplexCellBlock block = new ComplexCellBlock(columns(), 1);
            cells.add(block);
            return block;
        }

        private void ensureCapacity(int row)
        {
            while (row >= cells.size())
                cells.add(null);
        }

        protected ComplexCellBlock cellBlock(int row)
        {
            return row >= cells.size() ? null : cells.get(row);
        }

        protected int cellBlockBase(int row)
        {
            return 0;
        }

        // Swap row i and j
        protected void swapCells(int i, int j)
        {
            int max = Math.max(i, j);
            if (max >= cells.size())
                ensureCapacity(max);

            ComplexCellBlock block = cells.get(j);
            move(i, j);
            cells.set(i, block);
        }

        // Merge row i into j
        protected void mergeCells(int i, int j, int nowInSec)
        {
            assert i > j;
            if (i >= cells.size())
                return;

            ComplexCellBlock b1 = cells.get(i);
            ComplexCellBlock b2 = cells.get(j);
            ComplexCellBlock merged = new ComplexCellBlock(columns(), 1);

            int idxMerged = 0;
            int s = columns().complexColumnCount();
            for (int k = 0; k < s; k++)
            {
                ColumnDefinition column = columns().getComplex(k);
                Comparator<CellPath> comparator = column.cellPathComparator();

                merged.cellIdx[2 * k] = idxMerged;

                int idx1 = b1.cellIdx[2 * k];
                int end1 = b1.cellIdx[2 * k + 1];
                int idx2 = b2.cellIdx[2 * k];
                int end2 = b2.cellIdx[2 * k + 1];

                while (idx1 < end1 && idx2 < end2)
                {
                    int cmp = idx1 >= end1 ? 1
                            : (idx2 >= end2 ? -1
                            : comparator.compare(b1.complexPaths[idx1], b2.complexPaths[idx2]));

                    if (cmp == 0)
                        merge(b1, idx1++, b2, idx2++, merged, idxMerged++, nowInSec);
                    else if (cmp < 0)
                        copy(b1, idx1++, merged, idxMerged++);
                    else
                        copy(b2, idx2++, merged, idxMerged++);
                }

                merged.cellIdx[2 * k + 1] = idxMerged;
            }
        }

        private void copy(ComplexCellBlock fromBlock, int fromIdx, ComplexCellBlock toBlock, int toIdx)
        {
            fromBlock.data.moveCell(fromIdx, toBlock.data, toIdx);
            toBlock.ensureComplexPathsCapacity(toIdx);
            toBlock.complexPaths[toIdx] = fromBlock.complexPaths[fromIdx];
        }

        private void merge(ComplexCellBlock b1, int idx1, ComplexCellBlock b2, int idx2, ComplexCellBlock mergedBlock, int mergedIdx, int nowInSec)
        {
            CellData.mergeCell(b1.data, idx1, b2.data, idx2, mergedBlock.data, mergedIdx, nowInSec);
            mergedBlock.ensureComplexPathsCapacity(mergedIdx);
            mergedBlock.complexPaths[mergedIdx] = b1.complexPaths[idx1];
        }

        // Move row i into j
        protected void moveCells(int i, int j)
        {
            int max = Math.max(i, j);
            if (max >= cells.size())
                ensureCapacity(max);

            cells.set(j, cells.get(i));
        }

        protected long cellDataUnsharedHeapSizeExcludingData()
        {
            long size = EMPTY_SIZE;
            for (ComplexCellBlock block : cells)
                if (block != null)
                    size += block.unsharedHeapSizeExcludingData();
            return size;
        }

        protected int dataCellSize()
        {
            int size = 0;
            for (ComplexCellBlock block : cells)
                if (block != null)
                    size += block.dataSize();
            return size;
        }

        protected void clearCellData()
        {
            for (ComplexCellBlock block : cells)
                if (block != null)
                    block.clear();
        }
    }

    private static class ComplexCellBlock
    {
        private final Columns columns;

        /*
         * For a given complex column c, we have to store an unknown number of
         * cells. So for each column of each row, we keep pointers (in data)
         * to the start and end of the cells for this column (cells for a given
         * columns are thus stored contiguously).
         * For instance, if columns has 'c' complex columns, the x-th column of
         * row 'n' will have it's cells in data at indexes
         *    [cellIdx[2 * (n * c + x)], cellIdx[2 * (n * c + x) + 1])
         */
        private int[] cellIdx;

        final CellData data;

        // Complex cells has also a path. Those are indexed like the cells in
        // data, through cellIdx.
        private CellPath[] complexPaths;

        public ComplexCellBlock(Columns columns, int rows)
        {
            this.columns = columns;

            int columnCount = columns.complexColumnCount();
            this.cellIdx = new int[columnCount * 2];

            // We start with an estimated 4 cells per complex column. The arrays
            // will grow if needed so this is just a somewhat random estimation.
            int cellCount =  columnCount * 4;
            this.data = new CellData(cellCount);
            this.complexPaths = new CellPath[cellCount];
        }

        public long unsharedHeapSizeExcludingData()
        {
            long size = ObjectSizes.sizeOfArray(cellIdx)
                      + data.unsharedHeapSizeExcludingData()
                      + ObjectSizes.sizeOfArray(complexPaths);

            for (int i = 0; i < complexPaths.length; i++)
                if (complexPaths[i] != null)
                    size += ((MemtableRowData.BufferCellPath)complexPaths[i]).unsharedHeapSizeExcludingData();
            return size;
        }

        public int dataSize()
        {
            int size = data.dataSize() + cellIdx.length * 4;

            for (int i = 0; i < complexPaths.length; i++)
                if (complexPaths[i] != null)
                    size += complexPaths[i].dataSize();

            return size;
        }

        private void ensureCapacity(int rowToSet)
        {
            int columnCount = columns.complexColumnCount();
            int originalCapacity = cellIdx.length / (2 * columnCount);
            if (rowToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);
            cellIdx = Arrays.copyOf(cellIdx, newCapacity * 2 * columnCount);
        }

        public void ensureComplexPathsCapacity(int idxToSet)
        {
            int originalCapacity = complexPaths.length;
            if (idxToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, idxToSet);
            complexPaths = Arrays.copyOf(complexPaths, newCapacity);
        }

        public void clear()
        {
            data.clear();
            Arrays.fill(cellIdx, 0);
            Arrays.fill(complexPaths, null);
        }
    }

    private static class ReusableCell extends CellData.ReusableCell
    {
        private ComplexCellBlock cellBlock;

        ReusableCell setTo(ComplexCellBlock cellBlock, ColumnDefinition column, int idx)
        {
            this.cellBlock = cellBlock;
            super.setTo(cellBlock.data, column, idx);
            return this;
        }

        @Override
        public CellPath path()
        {
            return cellBlock.complexPaths[idx];
        }
    }

    static class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private ComplexCellBlock cellBlock;
        private final ReusableCell cell = new ReusableCell();

        private int rowIdx;

        private int columnIdx;
        private int endColumnIdx;

        private int idx;
        private int endIdx;

        private ReusableIterator()
        {
        }

        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row, ColumnDefinition column)
        {
            if (dataBlock == null)
            {
                this.cellBlock = null;
                return null;
            }

            this.cellBlock = dataBlock.cellBlock(row);
            if (cellBlock == null)
                return null;

            columnIdx = dataBlock.columns.complexIdx(column);
            if (columnIdx < 0)
                return null;

            endColumnIdx = columnIdx + 1;
            rowIdx = dataBlock.cellBlockBase(row);

            resetCellIdx();
            return endIdx <= idx ? null : this;
        }

        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row)
        {
            if (dataBlock == null)
            {
                this.cellBlock = null;
                return null;
            }

            this.cellBlock = dataBlock.cellBlock(row);
            if (cellBlock == null)
                return null;

            columnIdx = 0;
            endColumnIdx = dataBlock.columns.complexColumnCount();

            rowIdx = dataBlock.cellBlockBase(row);

            findNextColumnWithCells();
            return columnIdx < endColumnIdx ? null : this;
        }

        private void findNextColumnWithCells()
        {
            while (columnIdx < endColumnIdx)
            {
                resetCellIdx();
                if (idx < endIdx)
                    return;
                ++columnIdx;
            }
        }

        private void resetCellIdx()
        {
            int i = rowIdx + 2 * columnIdx;
            idx = cellBlock.cellIdx[i];
            endIdx = cellBlock.cellIdx[i + 1];
        }

        public boolean hasNext()
        {
            if (columnIdx >= endColumnIdx)
                return false;

            if (idx < endIdx)
                return true;

            ++columnIdx;
            findNextColumnWithCells();

            return columnIdx < endColumnIdx;
        }

        public Cell next()
        {
            return cell.setTo(cellBlock, cellBlock.columns.getComplex(columnIdx), idx++);
        }
    }

    public class CellWriter
    {
        private int base;
        private int row;

        // Index of the next free slot in data.
        private int idx;

        public void addCell(ColumnDefinition column, ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            assert path != null;

            ComplexCellBlock cellBlock = cellBlockForWritting(row);
            int columnIdx = cellBlockBase(row) + 2 * columns.complexIdx(column);

            int start = cellBlock.cellIdx[columnIdx];
            int end = cellBlock.cellIdx[columnIdx + 1];
            if (end <= start)
            {
                // First cell for the complex column
                cellBlock.cellIdx[columnIdx] = idx;
                cellBlock.cellIdx[columnIdx + 1] = idx + 1;
            }
            else
            {
                cellBlock.cellIdx[columnIdx + 1] = idx + 1;
            }

            cellBlock.data.setCell(idx, value, timestamp, localDeletionTime, ttl);
            cellBlock.ensureComplexPathsCapacity(idx);
            cellBlock.complexPaths[idx] = path;
            ++idx;
        }

        public void setComplexDeletion(ColumnDefinition column, DeletionTime deletionTime)
        {
            int columnIdx = base + columns.complexIdx(column);
            ensureDelTimesCapacity(row);
            complexDelTimes.set(columnIdx, deletionTime);
        }

        public void endOfRow()
        {
            base += columns.complexColumnCount();
            ++row;
        }

        public void reset()
        {
            base = 0;
            idx = 0;
            row = 0;
            clearCellData();
            complexDelTimes.clear();
        }
    }
}
