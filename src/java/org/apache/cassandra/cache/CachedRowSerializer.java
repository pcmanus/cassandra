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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.io.IColumnSerializer.Flag.LOCAL;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;

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
 * NameOffset:          int
 * ValueOffset:         int
 * ValueLength:         int
 * ===========================
 * Column Data
 * ===========================
 * Name:                byte[]
 * Value:               byte[]
 * SerializationFlags:  byte
 * Misc:                ?
 * Timestamp:           long
 * ---------------------------
 * Misc Counter Column
 * ---------------------------
 * TSOfLastDelete:      long
 * ---------------------------
 * Misc Expiring Column
 * ---------------------------
 * TimeToLive:          int
 * LocalDeletionTime:   int
 * ===========================
 *
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

    public static final int COL_INDEX_SIZE = INT_SIZE // name offset
            + INT_SIZE   // value offsets
            + INT_SIZE;  // value length

    public static final int MAX_TIMESTAMP_POS = 0;
    public static final int LOCAL_DELETION_POS = LONG_SIZE;
    public static final int MARKED_FOR_DELETE_AT_POS = LOCAL_DELETION_POS + INT_SIZE;
    public static final int NUM_COLUMNS_POS = MARKED_FOR_DELETE_AT_POS + LONG_SIZE;

    public static ByteBuffer serialize(ColumnFamily cf)
    {
        Collection<IColumn> sortedColumns = cf.getSortedColumns();

        // this might be expensive
        int numColumns = sortedColumns.size();

        int serializedSize = getSerializedSize(sortedColumns);
        ByteBuffer row = ByteBuffer.allocate(serializedSize);

        serializeHeader(cf, numColumns, row);

        int dataOffset = HEADER_SIZE + numColumns * COL_INDEX_SIZE;

        serializeColumns(sortedColumns, dataOffset, row);
        row.position(0);

        return row;
    }

    private static int getSerializedSize(Collection<IColumn> columns)
    {
        int size = HEADER_SIZE;

        for (IColumn column : columns)
        {
            size += column.serializedSize(TypeSizes.NATIVE)
                    + SHORT_SIZE // instead of short name lenght we store an int offset
                    + INT_SIZE;  // name offset                    
        }

        return size;
    }

    private static void serializeColumns(Collection<IColumn> sortedColumns, int dataBaseOffset, ByteBuffer row)
    {
        int dataOffset = dataBaseOffset;
        int indexOffset = HEADER_SIZE;
        for (IColumn column : sortedColumns)
        {
            ByteBuffer name = column.name();
            int nameLength = name.remaining();
            ByteBuffer value = column.value();
            int valueLength = value.remaining();

            // write index entry
            row.position(indexOffset);


//            logger.info("Put dataoffset {} @ {}", dataOffset, row.position());
            row.putInt(dataOffset);
            row.putInt(dataOffset + nameLength);
            row.putInt(valueLength);

            indexOffset = row.position();

            // write data
            ByteBufferUtil.arrayCopy(name, name.position(), row, dataOffset, nameLength);
            dataOffset += nameLength;
            ByteBufferUtil.arrayCopy(value, value.position(), row, dataOffset, valueLength);
            dataOffset += valueLength;

            row.position(dataOffset);
            row.put((byte) column.serializationFlags());
            if (column instanceof CounterColumn)
            {
                row.putLong(((CounterColumn) column).timestampOfLastDelete());
            }
            else if (column instanceof ExpiringColumn)
            {
                row.putInt(((ExpiringColumn) column).getTimeToLive());
                row.putInt(column.getLocalDeletionTime());
            }
            row.putLong(column.timestamp());
            dataOffset = row.position();
        }

        assert row.remaining() == 0 : "Invalid row size: " + row.remaining(); 
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

    public static ByteBuffer getFirstColumnName(ByteBuffer row)
    {
        return getName(row, 0);
    }
    
    public static ByteBuffer getLastColumnName(ByteBuffer row)
    {
        int lastColumnIndex = row.getInt(NUM_COLUMNS_POS) - 1;
        return getName(row, lastColumnIndex);
    }

    public static ByteBuffer getName(ByteBuffer row, int columnIndex)
    {
        int indexOffset = HEADER_SIZE + columnIndex * COL_INDEX_SIZE;
        int valueOffset = row.getInt(indexOffset + INT_SIZE);
        int nameOffset = row.getInt(indexOffset);
        
        ByteBuffer name = row.duplicate();
        name.limit(valueOffset);
        name.position(nameOffset);
        
        return name;
    }
    
    public static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        int lastColumnIndex = row.getInt(NUM_COLUMNS_POS) - 1;
        return binarySearch(row, name, comparator, 0, lastColumnIndex);
    }

    private static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator, int low, int high)
    {
        ByteBuffer midKey = row.duplicate();
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int indexOffset = HEADER_SIZE + mid * COL_INDEX_SIZE;
            int nameOffset = row.getInt(indexOffset);
            int valueOffset = row.getInt(indexOffset + INT_SIZE);

            midKey.limit(valueOffset);
            midKey.position(nameOffset);

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

    public static IColumn deserializeColumn(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        int low = 0;
        int high = row.getInt(NUM_COLUMNS_POS) - 1;

        ByteBuffer midKey = row.duplicate();
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int indexOffset = HEADER_SIZE + mid * COL_INDEX_SIZE;
            int nameOffset = row.getInt(indexOffset);
            int valueOffset = row.getInt(indexOffset + INT_SIZE);

            midKey.limit(valueOffset);
            midKey.position(nameOffset);

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
                return createColumn(row, midKey, valueOffset, row.getInt(indexOffset + LONG_SIZE));
            }
        }

        return null;
    }

    public static void appendRow(ByteBuffer row, ColumnFamily cf, ByteBuffer startColumn, ByteBuffer finishColumn, 
                                 int limit, boolean reversed, Comparator<ByteBuffer> comparator)
    {
        int numColumns = row.getInt(NUM_COLUMNS_POS);

        if (numColumns == 0)
            return;

        int startIndex, endIndex;
        int step = reversed ? -1 : 1;      
        if (startColumn.remaining() == 0 && finishColumn.remaining() == 0) 
        {
            if (reversed)
            {
                startIndex = numColumns - 1;
                endIndex = ((limit > 0) ? Math.max(startIndex - limit + 1, 0) : 0);                
            }
            else 
            {
                startIndex = 0;
                endIndex = ((limit > 0) ? Math.min(limit, numColumns) : numColumns) - 1;
            }
        }
        else if (startColumn.remaining() == 0)
        {
            int i = binarySearch(row, finishColumn, comparator);
            if (reversed)
            {
                startIndex = numColumns - 1;
                endIndex = i < 0 ? (-i - 1) : i;
                if (limit > 0)
                    endIndex = Math.max(startIndex - limit + 1, endIndex); 
            }
            else 
            {
                startIndex = 0;
                endIndex = i < 0 ? (-i - 2) : i;
                if (limit > 0)
                    endIndex = Math.min(limit - 1, endIndex); 
            }
        }
        else if (finishColumn.remaining() == 0)
        {
            int i = binarySearch(row, startColumn, comparator);
            if (reversed)
            {
                startIndex = i < 0 ? (-i - 2) : i;
                endIndex = ((limit > 0) ? Math.max(startIndex - limit + 1, 0) : 0);
            }
            else 
            {
                startIndex = i < 0 ? (-i - 1) : i;
                endIndex = ((limit > 0) ? Math.min(startIndex + limit, numColumns) : numColumns) - 1;
            }
        }
        else 
        {
            int i = binarySearch(row, startColumn, comparator);
            if (reversed)
            {
                startIndex = i < 0 ? (-i - 2) : i;
                int j = binarySearch(row, finishColumn, comparator, 0, startIndex);
                endIndex = j < 0 ? (-j - 1) : j;
                if (limit > 0)
                    endIndex = Math.max(startIndex - limit + 1, endIndex);
            }
            else 
            {
                startIndex = i < 0 ? (-i - 1) : i;
                int j = binarySearch(row, finishColumn, comparator, startIndex, numColumns - 1);
                endIndex = j < 0 ? (-j - 2) : j;
                if (limit > 0)
                    endIndex = Math.min(startIndex + limit - 1, endIndex);
            }
        }

        if (reversed && startIndex < endIndex || 
            !reversed && endIndex < startIndex )
            return;

        while (true)
        {
            Column column = createColumn(row, startIndex);
            cf.addColumn(column);

            if (startIndex == endIndex)
                return;
            
            startIndex += step;           
        }
    }

    public static Column createColumn(ByteBuffer row, int columnIndex)
    {
        int indexOffset = HEADER_SIZE + columnIndex * 3 * INT_SIZE;
        int nameOffset = row.getInt(indexOffset);
        indexOffset += INT_SIZE;
        int valueOffset = row.getInt(indexOffset);
        indexOffset += INT_SIZE;
        int valueLength = row.getInt(indexOffset);
        indexOffset += INT_SIZE;
        return createColumn(row, nameOffset, valueOffset, valueLength);
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

    private static Column createColumn(ByteBuffer row, int nameOffset, int valueOffset, int valueLength)
    {
        ByteBuffer name = row.duplicate();
        name.position(nameOffset);
        name.limit(valueOffset);
        return createColumn(row, name, valueOffset, valueLength);
    }

    private static Column createColumn(ByteBuffer row, ByteBuffer name, int valueOffset, int valueLength)
    {
        int flagsOffset = valueOffset + valueLength;
        ByteBuffer value = row.duplicate();
        value.position(valueOffset);
        value.limit(flagsOffset);

        row = row.duplicate();

        row.position(flagsOffset);
        byte serializationFlags = row.get();
        if ((serializationFlags & ColumnSerializer.COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = row.getLong();
            long ts = row.getLong();
            return (CounterColumn.create(name, value, ts, timestampOfLastDelete, LOCAL));
        }
        else if ((serializationFlags & ColumnSerializer.EXPIRATION_MASK) != 0)
        {
            int ttl = row.getInt();
            int expiration = row.getInt();
            long ts = row.getLong();
            int expireBefore = (int) (System.currentTimeMillis() / 1000);
            return (ExpiringColumn.create(name, value, ts, ttl, expiration, expireBefore, LOCAL));
        }
        else
        {
            long ts = row.getLong();
            return ((serializationFlags & ColumnSerializer.COUNTER_UPDATE_MASK) != 0
                    ? new CounterUpdateColumn(name, value, ts)
                    : ((serializationFlags & ColumnSerializer.DELETION_MASK) == 0
                    ? new Column(name, value, ts)
                    : new DeletedColumn(name, value, ts)));
        }
    }
}
