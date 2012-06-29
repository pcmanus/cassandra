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
package org.apache.cassandra.db.columniterator;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.cache.CachedRowSerializer;
import org.apache.cassandra.cache.CachedRow;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;

public class CachedRowSliceIterator extends SimpleAbstractColumnIterator implements OnDiskAtomIterator
{
    private final boolean reversed;
    private final CFMetaData metadata;

    private final DecoratedKey key;
    private ColumnFamily cf;
    
    private int currentIndex = Integer.MIN_VALUE;
    private int currentSliceIndex = Integer.MIN_VALUE;
    
    private ByteBuffer row;
    private ColumnSlice[] slices;

    public CachedRowSliceIterator(CFMetaData metadata, CachedRow cachedRow, DecoratedKey key, ColumnSlice[] slices, boolean reversed)
    {
        this.key = key;
        this.slices = slices;
        this.reversed = reversed;
        this.metadata = metadata;

        this.row = cachedRow.getBuffer();

        try
        {
            read(row);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public CachedRowSliceIterator(CFMetaData metadata, CachedRow cachedRow, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this(metadata, cachedRow, key, new ColumnSlice[]{new ColumnSlice(startColumn, finishColumn)}, reversed);
    }

    private void read(ByteBuffer row) throws IOException
    {
        cf = ColumnFamily.create(metadata, ArrayBackedSortedColumns.factory(), reversed);
        CachedRowSerializer.deserializeFromCachedRowNoColumns(row, cf);
    }

    private boolean isColumnNeeded(ColumnSlice currentSlice, IColumn column)
    {
        ByteBuffer startColumn = currentSlice.start;
        ByteBuffer finishColumn = currentSlice.finish;

        if (startColumn.remaining() == 0 && finishColumn.remaining() == 0)
            return true;
        else if (startColumn.remaining() == 0 && !reversed)
            return metadata.comparator.compare(column.name(), finishColumn) <= 0;
        else if (startColumn.remaining() == 0 && reversed)
            return metadata.comparator.compare(column.name(), finishColumn) >= 0;
        else if (finishColumn.remaining() == 0 && !reversed)
            return metadata.comparator.compare(column.name(), startColumn) >= 0;
        else if (finishColumn.remaining() == 0 && reversed)
            return metadata.comparator.compare(column.name(), startColumn) <= 0;
        else if (!reversed)
            return metadata.comparator.compare(column.name(), startColumn) >= 0 && metadata.comparator.compare(column.name(), finishColumn) <= 0;
        else // if reversed
            return metadata.comparator.compare(column.name(), startColumn) <= 0 && metadata.comparator.compare(column.name(), finishColumn) >= 0;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    protected IColumn computeNext()
    {
        if (reversed)
        {
            if (currentSliceIndex == Integer.MIN_VALUE)
                currentSliceIndex = slices.length - 1;

            if (currentSliceIndex < 0)
                return null;

            for (int i = currentSliceIndex; i >= 0; i--)
            {
                ColumnSlice currentSlice = slices[i];
                IColumn column = computeNextColumnInSlice(currentSlice);
                if (column != null && isColumnNeeded(currentSlice, column))
                    return column;

                // lookahead: break early if the slice starts beyond the end of the row
                if (i > 0 && metadata.comparator.compare(slices[i - 1].start, CachedRowSerializer.getFirstColumnName(row)) < 0)
                    return (IColumn) endOfData();
            }

            return (IColumn) endOfData();
        }
        else 
        {
            if (currentSliceIndex == Integer.MIN_VALUE)
                currentSliceIndex = 0;

            if (currentSliceIndex >= slices.length) 
                return null;
            
            for (int i = currentSliceIndex; i < slices.length; i++)
            {
                ColumnSlice currentSlice = slices[i];
                IColumn column = computeNextColumnInSlice(currentSlice);
                if (column != null && isColumnNeeded(currentSlice, column))
                    return column;

                // lookahead: break early if the slice starts beyond the end of the row
                if (i < slices.length - 1 && metadata.comparator.compare(slices[i + 1].start, CachedRowSerializer.getLastColumnName(row)) > 0)
                    return (IColumn) endOfData();
            }

            return (IColumn) endOfData();
        }
    }
    
    private IColumn computeNextColumnInSlice(ColumnSlice slice)
    {
        if (currentIndex == Integer.MIN_VALUE) {
            ByteBuffer startColumn = slice.start;
            if (startColumn.remaining() > 0)
            {
                int i = CachedRowSerializer.binarySearch(row, startColumn, metadata.comparator);
                currentIndex = ((i < 0) ? reversed ? (-i - 2) : (-i - 1) : i);
            }
            else
            {
                currentIndex = reversed ? CachedRowSerializer.getColumnCount(row) - 1 : 0;
            }
        }

        int step;
        if (reversed)
        {
            if (currentIndex < 0)
            {
                // reset index for a potential next slice
                currentIndex = Integer.MIN_VALUE;
                return null;
            }
            step = -1;
        }
        else
        {
            if (currentIndex > CachedRowSerializer.getColumnCount(row) - 1)
            {
                currentIndex = Integer.MIN_VALUE;
                return null;
            }

            step = 1;
        }

        Column column = CachedRowSerializer.createColumn(row, currentIndex);
        currentIndex += step;
        return column;
    }
}
