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

// TODO: need to abstract have have a subclass for counters too
public class RowDataBlock
{
    private final SimpleRowDataBlock simpleData;
    private final ComplexRowDataBlock complexData;

    public RowDataBlock(Columns columns, int rows)
    {
        this.simpleData = columns.hasSimple() ? new SimpleRowDataBlock(columns, rows) : null;
        this.complexData = columns.hasComplex() ? new ComplexRowDataBlock(columns, rows) : null;
    }

    public Columns columns()
    {
        if (simpleData != null)
            return simpleData.columns();
        if (complexData != null)
            return complexData.columns();
        return Columns.NONE;
    }

    public CellData.ReusableCell reusableSimpleCell()
    {
        return simpleData == null ? null : simpleData.reusableCell();
    }

    public ComplexRowDataBlock.ReusableIterator reusableComplexCells()
    {
        return complexData == null ? null : complexData.reusableComplexCells();
    }

    public DeletionTimeArray.Cursor complexDeletionCursor()
    {
        return complexData == null ? null : complexData.complexDeletionCursor();
    }

    public ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    // Swap row i and j
    public void swap(int i, int j)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public abstract static class Writer implements Rows.Writer
    {
        protected int row;

        protected SimpleRowDataBlock.CellWriter simpleWriter;
        protected ComplexRowDataBlock.CellWriter complexWriter;

        protected Writer()
        {
        }

        protected Writer(RowDataBlock data)
        {
            updateWriter(data);
        }

        protected void updateWriter(RowDataBlock data)
        {
            this.simpleWriter = data.simpleData == null ? null : data.simpleData.cellWriter();
            this.complexWriter = data.complexData == null ? null : data.complexData.cellWriter();
        }

        public Writer reset()
        {
            row = 0;

            if (simpleWriter != null)
                simpleWriter.reset();
            if (complexWriter != null)
                complexWriter.reset();

            return this;
        }

        public void addCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            assert !isCounter;
            if (column.isComplex())
                complexWriter.addCell(column, value, timestamp, localDeletionTime, ttl, path);
            else
                simpleWriter.addCell(column, value, timestamp, localDeletionTime, ttl);
        }

        public void setComplexDeletion(ColumnDefinition c, DeletionTime complexDeletion)
        {
            if (complexDeletion.isLive())
                return;

            complexWriter.setComplexDeletion(c, complexDeletion);
        }

        public void endOfRow()
        {
            ++row;
            if (simpleWriter != null)
                simpleWriter.endOfRow();
            if (complexWriter != null)
                complexWriter.endOfRow();
        }
    }

    class ReusableIterator extends UnmodifiableIterator<Cell> implements Iterator<Cell>
    {
        private SimpleRowDataBlock.ReusableIterator simpleIterator;
        private ComplexRowDataBlock.ReusableIterator complexIterator;

        public ReusableIterator()
        {
            this.simpleIterator = simpleData == null ? null : simpleData.reusableIterator();
            this.complexIterator = complexData == null ? null : complexData.reusableIterator();
        }

        public ReusableIterator setTo(int row)
        {
            if (simpleIterator != null)
                simpleIterator.setTo(row);
            if (complexIterator != null)
                complexIterator.setTo(row);

            return this;
        }

        public boolean hasNext()
        {
            return (simpleIterator != null && simpleIterator.hasNext())
                || (complexIterator != null && complexIterator.hasNext());
        }

        public Cell next()
        {
            return simpleIterator != null && simpleIterator.hasNext()
                 ? simpleIterator.next()
                 : complexIterator.next();
        }
    }
}
