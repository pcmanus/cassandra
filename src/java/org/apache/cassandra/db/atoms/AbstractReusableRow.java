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

public abstract class AbstractReusableRow implements Row
{
    private CellData.ReusableCell simpleCell;
    private ComplexRowDataBlock.ReusableIterator complexCells;
    private DeletionTimeArray.Cursor complexDeletionCursor;
    private RowDataBlock.ReusableIterator iterator;

    public AbstractReusableRow()
    {
    }

    protected abstract int row();
    protected abstract RowDataBlock data();

    private CellData.ReusableCell simpleCell()
    {
        if (simpleCell == null)
            simpleCell = SimpleRowDataBlock.reusableCell();
        return simpleCell;
    }

    private ComplexRowDataBlock.ReusableIterator complexCells()
    {
        if (complexCells == null)
            complexCells = ComplexRowDataBlock.reusableComplexCells();
        return complexCells;
    }

    private DeletionTimeArray.Cursor complexDeletionCursor()
    {
        if (complexDeletionCursor == null)
            complexDeletionCursor = ComplexRowDataBlock.complexDeletionCursor();
        return complexDeletionCursor;
    }

    private RowDataBlock.ReusableIterator reusableIterator()
    {
        if (iterator == null)
            iterator = RowDataBlock.reusableIterator();
        return iterator;
    }

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    public Columns columns()
    {
        return data().columns();
    }

    public boolean isEmpty()
    {
        return timestamp() == Rows.NO_TIMESTAMP && !iterator().hasNext() && !hasComplexDeletion();
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        return data().simpleData == null ? null : simpleCell().setTo(data().simpleData.data, c, (row() * columns().simpleColumnCount()) + columns().simpleIdx(c));
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        assert c.isComplex();
        return complexCells().setTo(data().complexData, row(), c);
    }

    public boolean hasComplexDeletion()
    {
        return data().hasComplexDeletion(row());
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        assert c.isComplex();
        if (data().complexData == null)
            return DeletionTime.LIVE;

        int idx = data().complexData.complexDeletionIdx(row(), c);
        return idx < 0
             ? DeletionTime.LIVE
             : complexDeletionCursor().setTo(data().complexData.complexDelTimes, idx);
    }

    public Iterator<Cell> iterator()
    {
        return reusableIterator().setTo(data(), row());
    }

    public Row takeAlias()
    {
        final ClusteringPrefix clustering = clustering().takeAlias();
        final long timestamp = timestamp();

        return new AbstractReusableRow()
        {
            private final RowDataBlock data = data();
            private final int row = row();

            protected RowDataBlock data()
            {
                return data;
            }

            protected int row()
            {
                return row;
            }

            public ClusteringPrefix clustering()
            {
                return clustering;
            }

            public long timestamp()
            {
                return timestamp;
            }

            @Override
            public Row takeAlias()
            {
                return this;
            }
        };
    }
}
