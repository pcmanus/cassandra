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
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;

public class CachedRow implements IRowCacheEntry
{
    private static final int COL_INDEX_SIZE = TypeSizes.NATIVE.sizeof(Integer.MAX_VALUE);

    private final ByteBuffer buffer;

    private final int columnCount;
    private final long maxTimestamp;
    private final int indexBaseOffset;

    private CachedRow(ByteBuffer buffer, int columnCount, long maxTimestamp, int indexOffset)
    {
        assert buffer != null;
        this.buffer = buffer;
        this.columnCount = columnCount;
        this.maxTimestamp = maxTimestamp;
        this.indexBaseOffset = indexOffset;
    }

    /**
     * Format:
     * ==============================
     * Deletion Info:   DeletionInfo
     * Column offsets:         int[]
     * Columns:             Column[]
     * ==============================
     */
    public static CachedRow serialize(ColumnFamily cf)
    {
        try
        {
            IColumnSerializer serializer = cf.getColumnSerializer();
            ByteBuffer row = ByteBuffer.allocate(getSerializedSize(cf));
            int numColumns = cf.getColumnCount();

            ByteBufferDataOutput out = new ByteBufferDataOutput(row);
            DeletionInfo.serializer().serialize(cf.deletionInfo(), out, MessagingService.current_version);

            int indexOffset = out.buffer().position();

            ByteBufferDataOutput data = new ByteBufferDataOutput(row);
            data.buffer().position(indexOffset + numColumns * COL_INDEX_SIZE);

            for (IColumn column : cf)
            {
                // Index entry
                out.writeInt(data.buffer().position());

                // Data
                serializer.serialize(column, data);
            }

            return new CachedRow(row, numColumns, cf.maxTimestamp(), indexOffset);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private static int getSerializedSize(ColumnFamily cf)
    {
        long size = DeletionInfo.serializer().serializedSize(cf.deletionInfo(), MessagingService.current_version);
        for (IColumn column : cf)
        {
            size += column.serializedSize(TypeSizes.NATIVE)
                    + COL_INDEX_SIZE;  // index entry
        }
        return (int)size; // that'd better fit
    }

    public long getMaxTimestamp()
    {
        return maxTimestamp;
    }

    public DeletionInfo deletionInfo(Comparator<ByteBuffer> comparator)
    {
        try
        {
            return DeletionInfo.serializer().deserialize(new ByteBufferDataInput(buffer), MessagingService.current_version, comparator);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private int columnOffset(int idx)
    {
        int indexOffset = indexBaseOffset + idx * COL_INDEX_SIZE;
        return buffer.getInt(indexOffset);
    }

    private ByteBuffer getColumnName(int idx)
    {
        int columnOffset = columnOffset(idx);
        int nameLength = buffer.getShort(columnOffset) & 0xFFFF;
        int nameOffset = columnOffset + 2;
        return (ByteBuffer)buffer.duplicate().position(nameOffset).limit(nameOffset + nameLength);
    }

    // package accessible for test only
    int binarySearch(ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        return binarySearch(name, comparator, 0, columnCount - 1);
    }

    private int binarySearch(ByteBuffer name, Comparator<ByteBuffer> comparator, int low, int high)
    {
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            ByteBuffer midKey = getColumnName(mid);

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

    public IColumn getColumn(ByteBuffer name, Comparator<ByteBuffer> comparator, IColumnSerializer serializer) throws IOException
    {
        int idx = binarySearch(name, comparator);
        return idx < 0 ? null : getColumn(idx, serializer);
    }

    private IColumn getColumn(int idx, IColumnSerializer serializer) throws IOException
    {
        int columnOffset = columnOffset(idx);
        ByteBufferDataInput in = new ByteBufferDataInput(buffer);
        in.buffer().position(columnOffset);
        return serializer.deserialize(in);
    }

    public Iterator<IColumn> iterator(ColumnSlice slice, boolean reversed, Comparator<ByteBuffer> comparator, final IColumnSerializer serializer)
    {
        if (columnCount == 0)
            return Iterators.<IColumn>emptyIterator();

        ByteBuffer low = reversed ? slice.finish : slice.start;
        ByteBuffer high = reversed ? slice.start : slice.finish;

        // The first idx to include
        int lowIdx = low.remaining() == 0 ? 0 : binarySearch(low, comparator);
        if (lowIdx < 0)
            lowIdx = -lowIdx - 1;

        // The last idx to include
        int highIdx = high.remaining() == 0 ? columnCount - 1 : binarySearch(high, comparator);
        if (highIdx < 0)
            highIdx = -highIdx - 2;

        final ByteBufferDataInput in = new ByteBufferDataInput(buffer);
        final int startIdx = reversed ? highIdx : lowIdx;
        final int stopIdx = reversed ? lowIdx : highIdx;
        final int step = reversed ? -1 : 1;
        final CachedRow that = this;

        return new AbstractIterator<IColumn>()
        {
            private int currentIdx = startIdx;

            protected IColumn computeNext()
            {
                if (currentIdx == stopIdx + step)
                    return endOfData();

                int columnOffset = that.columnOffset(currentIdx);
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
