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
    protected final RowDataBlock data;

    private CellData.ReusableCell simpleCell;
    private ComplexRowDataBlock.ReusableIterator complexCells;
    private DeletionTimeArray.Cursor complexDeletionCursor;
    private RowDataBlock.ReusableIterator iterator;

    public AbstractReusableRow(RowDataBlock data)
    {
        this.data = data;
    }

    private CellData.ReusableCell simpleCell()
    {
        if (simpleCell == null)
            simpleCell = data.reusableSimpleCell();
        return simpleCell;
    }

    private ComplexRowDataBlock.ReusableIterator complexCells()
    {
        if (complexCells == null)
            complexCells = data.reusableComplexCells();
        return complexCells;
    }

    private DeletionTimeArray.Cursor complexDeletionCursor()
    {
        if (complexDeletionCursor == null)
            complexDeletionCursor = data.complexDeletionCursor();
        return complexDeletionCursor;
    }

    private RowDataBlock.ReusableIterator reusableIterator()
    {
        if (iterator == null)
            iterator = data.reusableIterator();
        return iterator;
    }

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    protected abstract int row();

    public Columns columns()
    {
        return data.columns();
    }

    public boolean isEmpty()
    {
        // TODO: can't we get rid of isEmpty?!
        throw new UnsupportedOperationException();
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        return simpleCell() == null ? null : simpleCell().setToPosition(c, (row() * data.columns().simpleColumnCount()) + data.columns().simpleIdx(c));
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        assert c.isComplex();
        return complexCells() == null ? null : complexCells().setTo(row(), c);
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        assert c.isComplex();
        return complexDeletionCursor() == null
             ? DeletionTime.LIVE
             : complexDeletionCursor().setTo((row() * data.columns().complexColumnCount()) + data.columns().complexIdx(c));
    }

    public Iterator<Cell> iterator()
    {
        return reusableIterator().setTo(row());
    }

    public Row takeAlias()
    {
        final ClusteringPrefix clustering = clustering().takeAlias();
        final long timestamp = timestamp();

        return new AbstractReusableRow(data)
        {
            protected int row()
            {
                return row();
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
