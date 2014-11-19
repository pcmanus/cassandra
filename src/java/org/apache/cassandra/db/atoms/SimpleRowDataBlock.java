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
class SimpleRowDataBlock
{
    private final Columns columns;
    private final CellData data;

    public SimpleRowDataBlock(Columns columns, int rows)
    {
        this.columns = columns;
        this.data = new CellData(rows * columns.simpleColumnCount());
    }

    public Columns columns()
    {
        return columns;
    }

    public CellData.ReusableCell reusableCell()
    {
        return new CellData.ReusableCell(data);
    }

    public CellWriter cellWriter()
    {
        return new CellWriter();
    }

    public ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private final int columnCount;
        private final CellData.ReusableCell cell = new CellData.ReusableCell(data);

        private int base;
        private int column;

        public ReusableIterator()
        {
            this.columnCount = columns.simpleColumnCount();
        }

        public ReusableIterator setTo(int row)
        {
            this.base = row * columnCount;
            this.column = 0;
        }

        public boolean hasNext()
        {
            while (column < columnCount && !data.hasCell(base + column))
                ++column;

            return column < columnCount;
        }

        public Cell next()
        {
            return cell.setToPosition(columns.get(column), base + column);
        }
    }

    class CellWriter
    {
        private int base;

        public void addCell(ColumnDefinition column, ByteBuffer value, long timestamp, int localDeletionTime, int ttl)
        {
            // TODO: we could slightly optimize the columns.idx() calls on the assumption that cells comes in columns order
            int idx = base + columns.idx(column);
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
