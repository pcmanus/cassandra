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
package org.apache.cassandra.cache;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.utils.*;

/**
 * Format:
 * <p/>
 * ===========================
 * Header
 * ===========================
 * MaxTimestamp:        long
 * LocalDeletionTime:   int
 * MarkedForDeleteAt:   long
 * NumColumns:          int
 * ===========================
 * Column Index
 * ===========================
 * Offset:              int[]
 * Columns:          Column[]
 * ===========================
 */
public class CachedRowSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(CachedRowSerializer.class);

    private static final int LONG_SIZE = TypeSizes.NATIVE.sizeof(Long.MAX_VALUE);
    private static final int INT_SIZE = TypeSizes.NATIVE.sizeof(Integer.MAX_VALUE);
    private static final int SHORT_SIZE = TypeSizes.NATIVE.sizeof(Short.MAX_VALUE);

    public static final int HEADER_SIZE = LONG_SIZE // max ts
            + INT_SIZE   // local deletion
            + LONG_SIZE  // marked for delete
            + INT_SIZE;  // num columns

    public static final int COL_INDEX_SIZE = INT_SIZE; // name offset

    public static final int MAX_TIMESTAMP_POS = 0;
    public static final int LOCAL_DELETION_POS = LONG_SIZE;
    public static final int MARKED_FOR_DELETE_AT_POS = LOCAL_DELETION_POS + INT_SIZE;
    public static final int NUM_COLUMNS_POS = MARKED_FOR_DELETE_AT_POS + LONG_SIZE;

    public static ByteBuffer serialize(ColumnFamily cf)
    {
        // this might be expensive
        int numColumns = cf.getColumnCount();

        int serializedSize = getSerializedSize(cf);
        ByteBuffer row = ByteBuffer.allocate(serializedSize);
        row.mark();

        serializeHeader(cf, numColumns, row);

        ByteBufferDataOutput out = new ByteBufferDataOutput(row);
        out.buffer().position(HEADER_SIZE + numColumns * COL_INDEX_SIZE);

        try
        {
            serializeColumns(cf, row, out);
            return (ByteBuffer)row.reset();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private static int getSerializedSize(ColumnFamily cf)
    {
        int size = HEADER_SIZE;

        for (IColumn column : cf)
        {
            size += column.serializedSize(TypeSizes.NATIVE)
                    + INT_SIZE;  // index entry
        }

        return size;
    }

    private static void serializeColumns(ColumnFamily cf, ByteBuffer index, ByteBufferDataOutput out) throws IOException
    {
        IColumnSerializer serializer = cf.getColumnSerializer();
        for (IColumn column : cf)
        {
            // Index entry
            index.putInt(out.buffer().position());

            // Data
            serializer.serialize(column, out);
        }
    }

    private static void serializeHeader(ColumnFamily cf, int numColumns, ByteBuffer row)
    {
        row.putLong(cf.maxTimestamp());
        DeletionTime topLevelDeletion = cf.deletionInfo().getTopLevelDeletion();
        row.putInt(topLevelDeletion.localDeletionTime);
        row.putLong(topLevelDeletion.markedForDeleteAt);
        row.putInt(numColumns);
    }

    public static int getColumnCount(ByteBuffer row)
    {
        return row.getInt(NUM_COLUMNS_POS);
    }

    private static int columnOffset(ByteBuffer row, int idx)
    {
        int indexOffset = HEADER_SIZE + idx * COL_INDEX_SIZE;
        return row.getInt(indexOffset);
    }

    private static ByteBuffer getName(ByteBuffer row, int idx)
    {
        return getColumnName(row, idx);
    }

    private static int getUnsignedShort(ByteBuffer row, int idx)
    {
        return row.getShort(idx) & 0xFFFF;
    }

    private static ByteBuffer getColumnName(ByteBuffer row, int idx)
    {
        int columnOffset = columnOffset(row, idx);
        int nameLength = getUnsignedShort(row, columnOffset);

        int nameOffset = columnOffset + 2;
        ByteBuffer name = row.duplicate();
        name.position(nameOffset);
        name.limit(nameOffset + nameLength);
        return name;
    }

    public static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        int lastColumnIndex = getColumnCount(row) - 1;
        return binarySearch(row, name, comparator, 0, lastColumnIndex);
    }

    private static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator, int low, int high)
    {
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            ByteBuffer midKey = getColumnName(row, mid);

            int compare = comparator.compare(midKey, name);
            if (compare < 0)
            {
                low = mid + 1;
            }
            else if (compare > 0)
            {
                high = mid - 1;
            }
            else
            {
                return mid;
            }
        }
        return -(low + 1);
    }

    public static IColumn deserializeColumn(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator, IColumnSerializer serializer) throws IOException
    {
        int low = 0;
        int high = getColumnCount(row) - 1;

        int idx = binarySearch(row, name, comparator, 0, getColumnCount(row) - 1);
        return idx < 0 ? null : createColumn(row, idx, serializer);
    }

    public static IColumn createColumn(ByteBuffer row, int columnIndex, IColumnSerializer serializer) throws IOException
    {
        int columnOffset = columnOffset(row, columnIndex);
        ByteBufferDataInput in = new ByteBufferDataInput(row);
        in.buffer().position(columnOffset);
        return serializer.deserialize(in);
    }

    public static void deserializeFromCachedRowNoColumns(ByteBuffer row, ColumnFamily cf)
    {
        DeletionInfo deletionInfo = new DeletionInfo(row.getLong(MARKED_FOR_DELETE_AT_POS), row.getInt(LOCAL_DELETION_POS));
        cf.delete(deletionInfo);
    }

    public static long getMaxTimestamp(ByteBuffer buffer)
    {
        return buffer.getLong(MAX_TIMESTAMP_POS);
    }

    public static Iterator<IColumn> iterator(final ByteBuffer row, ColumnSlice slice, boolean reversed, Comparator<ByteBuffer> comparator, final IColumnSerializer serializer)
    {
        int numColumns = getColumnCount(row);
        if (numColumns == 0)
            return Iterators.<IColumn>emptyIterator();

        ByteBuffer low = reversed ? slice.finish : slice.start;
        ByteBuffer high = reversed ? slice.start : slice.finish;

        // The first idx to include
        int lowIdx = low.remaining() == 0 ? 0 : binarySearch(row, low, comparator);
        if (lowIdx < 0)
            lowIdx = -lowIdx - 1;

        // The last idx to include
        int highIdx = high.remaining() == 0 ? numColumns - 1 : binarySearch(row, high, comparator);
        if (highIdx < 0)
            highIdx = -highIdx - 2;

        final ByteBufferDataInput in = new ByteBufferDataInput(row);
        final int startIdx = reversed ? highIdx : lowIdx;
        final int stopIdx = reversed ? lowIdx : highIdx;
        final int step = reversed ? -1 : 1;

        return new AbstractIterator<IColumn>()
        {
            private int currentIdx = startIdx;

            protected IColumn computeNext()
            {
                if (currentIdx == stopIdx + step)
                    return endOfData();

                int columnOffset = columnOffset(row, currentIdx);
                in.buffer().position(columnOffset);
                try
                {
                    IColumn col = serializer.deserialize(in);
                    currentIdx += step;
                    return col;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }
}
