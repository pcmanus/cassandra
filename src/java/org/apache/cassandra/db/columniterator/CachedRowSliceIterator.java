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
    private final ColumnFamily cf;

    private final ByteBuffer row;
    private final ColumnSlice[] slices;

    private Iterator<IColumn> iter;
    private int currentSlice;

    public CachedRowSliceIterator(CFMetaData metadata, CachedRow cachedRow, DecoratedKey key, ColumnSlice[] slices, boolean reversed)
    {
        this.key = key;
        this.slices = slices;
        this.reversed = reversed;
        this.metadata = metadata;

        this.row = cachedRow.getBuffer();

        try
        {
            this.cf = read(row);
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

    private ColumnFamily read(ByteBuffer row) throws IOException
    {
        ColumnFamily cf = ColumnFamily.create(metadata, ArrayBackedSortedColumns.factory(), reversed);
        CachedRowSerializer.deserializeFromCachedRowNoColumns(row, cf);
        return cf;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    protected OnDiskAtom computeNext()
    {
        if (iter == null)
        {
            if (currentSlice >= slices.length)
                return endOfData();

            int c = currentSlice++;
            ColumnSlice slice = slices[reversed ? slices.length - c - 1: c];
            iter = CachedRowSerializer.iterator(row, slice, reversed, cf.getComparator(), cf.getColumnSerializer());
        }

        if (iter.hasNext())
            return iter.next();

        iter = null;
        return computeNext();
    }
}
