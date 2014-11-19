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

public class ArrayBackedPartition extends AbstractPartitionData implements CachedPartition
{
    private ArrayBackedPartition(CFMetaData metadata,
                                 DecoratedKey partitionKey,
                                 DeletionTime partitionLevelDeletion,
                                 RowDataBlock columnData,
                                 int intialRowCapacity)
    {
        super(metadata, partitionKey, new DeletionInfo(partitionLevelDeletion), columnData, initialRowCapacity);
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

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo.getTopLevelDeletion();
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
}
