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
 * Contains simple cells data for one or more rows.
 */
public class SimpleRowDataBlock
{
    final Columns columns;
    final CellData data;

    public SimpleRowDataBlock(Columns columns, int rows)
    {
        this.columns = columns;
        this.data = new CellData(rows * columns.simpleColumnCount());
    }

    public Columns columns()
    {
        return columns;
    }

    // Swap row i and j
    public void swap(int i, int j)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.swapCell(i * s + k, j * s + k);
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.mergeCell(i * s + k, j * s + k, nowInSec);
    }

    // Move row i into j
    public void move(int i, int j)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.moveCell(i * s + k, j * s + k);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return data.unsharedHeapSizeExcludingData();
    }

    public int dataSize()
    {
        return data.dataSize();
    }

    public CellWriter cellWriter()
    {
        return new CellWriter();
    }

    public static CellData.ReusableCell reusableCell()
    {
        return new CellData.ReusableCell();
    }

    public static ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    static class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private SimpleRowDataBlock dataBlock;
        private final CellData.ReusableCell cell = new CellData.ReusableCell();

        private int base;
        private int column;

        private ReusableIterator()
        {
        }

        public ReusableIterator setTo(SimpleRowDataBlock dataBlock, int row)
        {
            this.dataBlock = dataBlock;
            this.base = dataBlock == null ? -1 : row * dataBlock.columns.simpleColumnCount();
            this.column = 0;
            return this;
        }

        public boolean hasNext()
        {
            if (dataBlock == null)
                return false;

            int columnCount = dataBlock.columns.simpleColumnCount();
            while (column < columnCount && !dataBlock.data.hasCell(base + column))
                ++column;

            return column < columnCount;
        }

        public Cell next()
        {
            cell.setTo(dataBlock.data, dataBlock.columns.getSimple(column), base + column);
            ++column;
            return cell;
        }
    }

    public class CellWriter
    {
        private int base;

        public void addCell(ColumnDefinition column, ByteBuffer value, long timestamp, int localDeletionTime, int ttl)
        {
            // TODO: we could slightly optimize the columns.idx() calls on the assumption that cells comes in columns order
            int idx = base + columns.simpleIdx(column);
            data.setCell(idx, value, timestamp, localDeletionTime, ttl);
        }

        public void reset()
        {
            base = 0;
        }

        public void endOfRow()
        {
            base += columns.simpleColumnCount();
        }
    }
}
