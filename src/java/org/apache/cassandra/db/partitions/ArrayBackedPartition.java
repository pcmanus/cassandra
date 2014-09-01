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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.SearchIterator;

public class ArrayBackedPartition implements CachedPartition
{
    // For use in kindAndEOC
    private static final int ROW_FLAG = 0x00;
    private static final int OPEN_MARKER_FLAG = 0x10;
    private static final int CLOSE_MARKER_FLAG = 0x20;

    // TODO: currently, when we need capacity, we just allocate bigger arrays and copy everything.
    // For small number of rows this is probably fine, but the copy could be costly for bigger
    // partitions. One strategy could be to use fixed size block of data. When we need more data
    // we'd just allocate a new block. We could even imagine to have a pool of blocks to reduce
    // allocations even more.

    private final CFMetaData metadata;
    private final DecoratedKey partitionKey;
    private final DeletionTime partitionLevelDeletion;

    // number of atoms, rows and markers stored
    private int atoms;
    private int rows;
    private int markers;

    // clustering values (or prefix for range tombstone markers)
    private final int clusteringSize;
    private ByteBuffer[] clusterings;
    // Store a byte for each atom that represent 1) it's kind and 2) the EOC of it's clustering
    private byte[] kindAndEOC;

    // For each row, if it is covered by an open marker, the index to that marker
    private int[] openMarker;

    private long[] rowTimestamps;

    private final ColumnDataContainer columnData;

    private final DeletionTimeArray markersDelTimes;

    private ArrayBackedPartition(CFMetaData metadata,
                                 DecoratedKey partitionKey,
                                 DeletionTime partitionLevelDeletion,
                                 ColumnDataContainer columnData,
                                 DeletionTimeArray markersDelTimes)
    {
        this.metadata = metadata;
        this.clusteringSize = metadata.clusteringColumns().size();
        this.partitionKey = partitionKey;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.columnData = columnData;
        this.markersDelTimes = markersDelTimes;
    }

    public static ArrayBackedPartition accumulate(AtomIterator iterator)
    {
        return accumulate(iterator, 16, 16, 16);
    }

    public static ArrayBackedPartition accumulate(AtomIterator iterator, int atomCapacity, int rowsCapacity, int cellsCapacity)
    {
        // TODO
        throw new UnsupportedOperationException();
        //AtomContainer data = new AtomContainer(iterator.metadata(),
        //                                       iterator.columns(),
        //                                       iterator.staticColumns(),
        //                                       atomCapacity,
        //                                       rowsCapacity,
        //                                       cellsCapacity);

        //AtomContainer.WriteCursor writer = data.newWriteCursor();

        //// TODO: handle static row and partition level deletion

        //while (iterator.hasNext())
        //    Atoms.copy(iterator.next(), writer);

        //return new ArrayBackedPartition(data);
    }

    public boolean isEmpty()
    {
        return partitionLevelDeletion.isLive() && atoms == 0;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public SearchIterator<ClusteringPrefix, Row> searchIterator()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public AtomIterator atomIterator(Slices slices, boolean reversed)
    {
        // TODO
        throw new UnsupportedOperationException();
        //return slice.makeSliceIterator(data.newReadCursor());
    }

    public Atom tail()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public int rowCount()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // The number of live rows in this cached partition. But please note that this always
    // count expiring cells as live, see CFS.isFilterFullyCoveredBy for the reason of this.
    public int rowsWithNonTombstoneCells()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    private boolean isRow(int atom)
    {
        return ((kindAndEOC[atom] & 0xF) == ROW_FLAG);
    }

    private boolean isOpenMarker(int atom)
    {
        return ((kindAndEOC[atom] & 0xF) == OPEN_MARKER_FLAG);
    }

    private byte eoc(int atom)
    {
        return (byte)(kindAndEOC[atom] & 0x0F);
    }

    private void ensureCapacityForAtom(int atom)
    {
        int currentCapacity = clusterings.length / clusteringSize;
        if (atom < currentCapacity)
            return;

        int newCapacity = (currentCapacity * 3) / 2 + 1;
        clusterings = Arrays.copyOf(clusterings, newCapacity * clusteringSize);
        kindAndEOC = Arrays.copyOf(kindAndEOC, newCapacity);
    }

    private void ensureCapacityForRow(int row)
    {
        int currentCapacity = rowTimestamps.length;
        if (row < currentCapacity)
            return;

        int newCapacity = (currentCapacity * 3) / 2 + 1;

        openMarker = Arrays.copyOf(openMarker, newCapacity);
        rowTimestamps = Arrays.copyOf(rowTimestamps, newCapacity);
    }

    private void ensureCapacityForMarker(int marker)
    {
        int currentCapacity = markersDelTimes.size();
        if (marker < currentCapacity)
            return;

        markersDelTimes.resize((currentCapacity * 3) / 2 + 1);
    }
}
