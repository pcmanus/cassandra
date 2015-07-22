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
package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class ArrayBackedCachedPartition extends ArrayBackedPartition implements CachedPartition
{
    private final int createdAtInSec;

    private final int cachedLiveRows;
    private final int rowsWithNonExpiringCells;

    private final int nonTombstoneCellCount;
    private final int nonExpiringLiveCells;

    private ArrayBackedCachedPartition(CFMetaData metadata,
                                       DecoratedKey partitionKey,
                                       PartitionColumns columns,
                                       Row staticRow,
                                       List<Row> rows,
                                       DeletionInfo deletionInfo,
                                       EncodingStats stats,
                                       int createdAtInSec,
                                       int cachedLiveRows,
                                       int rowsWithNonExpiringCells,
                                       int nonTombstoneCellCount,
                                       int nonExpiringLiveCells)
    {
        super(metadata, partitionKey, columns, staticRow, rows, deletionInfo, stats);
        this.createdAtInSec = createdAtInSec;
        this.cachedLiveRows = cachedLiveRows;
        this.rowsWithNonExpiringCells = rowsWithNonExpiringCells;
        this.nonTombstoneCellCount = nonTombstoneCellCount;
        this.nonExpiringLiveCells = nonExpiringLiveCells;
    }

    /**
     * Creates an {@code ArrayBackedCachedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator got gather in memory.
     * @param nowInSec the time of the creation in seconds. This is the time at which {@link #cachedLiveRows} applies.
     * @return the created partition.
     */
    public static ArrayBackedCachedPartition create(UnfilteredRowIterator iterator, int nowInSec)
    {
        return create(iterator, 16, nowInSec);
    }

    /**
     * Creates an {@code ArrayBackedCachedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator got gather in memory.
     * @param initialRowCapacity sizing hint (in rows) to use for the created partition. It should ideally
     * correspond or be a good estimation of the number or rows in {@code iterator}.
     * @param nowInSec the time of the creation in seconds. This is the time at which {@link #cachedLiveRows} applies.
     * @return the created partition.
     */
    public static ArrayBackedCachedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity, int nowInSec)
    {
        CFMetaData metadata = iterator.metadata();
        boolean reversed = iterator.isReverseOrder();

        List<Row> rows = new ArrayList<>(initialRowCapacity);
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), metadata.comparator, reversed);

        int cachedLiveRows = 0;
        int rowsWithNonExpiringCells = 0;

        int nonTombstoneCellCount = 0;
        int nonExpiringLiveCells = 0;

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
            {
                Row row = (Row)unfiltered;
                rows.add(row);

                // Collect stats
                if (row.hasLiveData(nowInSec))
                    ++cachedLiveRows;

                boolean hasNonExpiringCell = false;
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext())
                {
                    Cell cell = cellIterator.next();
                    if (!cell.isTombstone())
                    {
                        ++nonTombstoneCellCount;
                        if (!cell.isExpiring())
                        {
                            hasNonExpiringCell = true;
                            ++nonExpiringLiveCells;
                        }
                    }
                }

                if (hasNonExpiringCell)
                    ++rowsWithNonExpiringCells;
            }
            else
            {
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
            }
        }

        if (reversed)
            Collections.reverse(rows);

        return new ArrayBackedCachedPartition(metadata,
                                              iterator.partitionKey(),
                                              iterator.columns(),
                                              iterator.staticRow(),
                                              rows,
                                              deletionBuilder.build(),
                                              iterator.stats(),
                                              nowInSec,
                                              cachedLiveRows,
                                              rowsWithNonExpiringCells,
                                              nonTombstoneCellCount,
                                              nonExpiringLiveCells);
    }

    public Row lastRow()
    {
        if (rows.isEmpty())
            return null;

        return rows.get(rows.size() - 1);
    }

    /**
     * The number of rows that were live at the time the partition was cached.
     *
     * See {@link ColumnFamilyStore#isFilterFullyCoveredBy} to see why we need this.
     *
     * @return the number of rows in this partition that were live at the time the
     * partition was cached (this can be different from the number of live rows now
     * due to expiring cells).
     */
    public int cachedLiveRows()
    {
        return cachedLiveRows;
    }

    /**
     * The number of rows in this cached partition that have at least one non-expiring
     * non-deleted cell.
     *
     * Note that this is generally not a very meaningful number, but this is used by
     * {@link DataLimits#hasEnoughLiveData} as an optimization.
     *
     * @return the number of row that have at least one non-expiring non-deleted cell.
     */
    public int rowsWithNonExpiringCells()
    {
        return rowsWithNonExpiringCells;
    }

    public int nonTombstoneCellCount()
    {
        return nonTombstoneCellCount;
    }

    public int nonExpiringLiveCells()
    {
        return nonExpiringLiveCells;
    }

    static class Serializer implements ISerializer<CachedPartition>
    {
        public void serialize(CachedPartition partition, DataOutputPlus out) throws IOException
        {
            int version = MessagingService.current_version;

            assert partition instanceof ArrayBackedCachedPartition;
            ArrayBackedCachedPartition p = (ArrayBackedCachedPartition)partition;

            out.writeVInt(p.createdAtInSec);
            out.writeVInt(p.cachedLiveRows);
            out.writeVInt(p.rowsWithNonExpiringCells);
            out.writeVInt(p.nonTombstoneCellCount);
            out.writeVInt(p.nonExpiringLiveCells);
            try (UnfilteredRowIterator iter = p.sliceableUnfilteredIterator())
            {
                SerializationHeader header = new SerializationHeader(iter.metadata(), iter.columns(), iter.stats());
                SerializationHeader.serializer.serializeForMessaging(header, out, version);
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, out, version, header, p.rowCount());
            }
        }

        public CachedPartition deserialize(DataInputPlus in) throws IOException
        {
            int version = MessagingService.current_version;

            // Note that it would be slightly simpler to just do
            //   ArrayBackedCachedPiartition.create(UnfilteredRowIteratorSerializer.serializer.deserialize(...));
            // However deserializing the header separatly is not a lot harder and allows us to:
            //   1) get the capacity of the partition so we can size it properly directly
            //   2) saves the creation of a temporary iterator: rows are directly written to the partition, which
            //      is slightly faster.

            int createdAtInSec = (int)in.readVInt();
            int cachedLiveRows = (int)in.readVInt();
            int rowsWithNonExpiringCells = (int)in.readVInt();
            int nonTombstoneCellCount = (int)in.readVInt();
            int nonExpiringLiveCells = (int)in.readVInt();

            SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, version);
            CFMetaData metadata = header.metadata();

            UnfilteredRowIteratorSerializer.Header partitionHeader = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(in, MessagingService.current_version, SerializationHelper.Flag.LOCAL, header);
            assert !partitionHeader.isReversed && partitionHeader.rowEstimate >= 0;

            MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(partitionHeader.partitionDeletion, metadata.comparator, false);
            List<Row> rows = new ArrayList<>(partitionHeader.rowEstimate);

            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, SerializationHelper.Flag.LOCAL, header, partitionHeader))
            {
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered.kind() == Unfiltered.Kind.ROW)
                        rows.add((Row)unfiltered);
                    else
                        deletionBuilder.add((RangeTombstoneMarker)unfiltered);
                }
            }

            return new ArrayBackedCachedPartition(metadata,
                                                  partitionHeader.key,
                                                  header.columns(),
                                                  partitionHeader.staticRow,
                                                  rows,
                                                  deletionBuilder.build(),
                                                  header.stats(),
                                                  createdAtInSec,
                                                  cachedLiveRows,
                                                  rowsWithNonExpiringCells,
                                                  nonTombstoneCellCount,
                                                  nonExpiringLiveCells);

        }

        public long serializedSize(CachedPartition partition)
        {
            int version = MessagingService.current_version;

            assert partition instanceof ArrayBackedCachedPartition;
            ArrayBackedCachedPartition p = (ArrayBackedCachedPartition)partition;

            try (UnfilteredRowIterator iter = p.sliceableUnfilteredIterator())
            {
                SerializationHeader header = new SerializationHeader(iter.metadata(), iter.columns(), iter.stats());
                return TypeSizes.sizeofVInt(p.createdAtInSec)
                     + TypeSizes.sizeofVInt(p.cachedLiveRows)
                     + TypeSizes.sizeofVInt(p.rowsWithNonExpiringCells)
                     + TypeSizes.sizeofVInt(p.nonTombstoneCellCount)
                     + TypeSizes.sizeofVInt(p.nonExpiringLiveCells)
                     + SerializationHeader.serializer.serializedSizeForMessaging(header, version)
                     + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, version, header, p.rowCount());
            }
        }
    }
}

