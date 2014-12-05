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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;

public class ArrayBackedPartition extends AbstractPartitionData implements CachedPartition
{
    protected ArrayBackedPartition(CFMetaData metadata,
                                   DecoratedKey partitionKey,
                                   DeletionTime deletionTime,
                                   PartitionColumns columns,
                                   int initialRowCapacity)
    {
        super(metadata, partitionKey, deletionTime, columns, initialRowCapacity);
    }

    public static ArrayBackedPartition create(AtomIterator iterator)
    {
        return create(iterator, 16);
    }

    public static ArrayBackedPartition create(AtomIterator iterator, int initialRowCapacity)
    {
        // TODO: we need to fix this if we continue to use this in ReadResponse
        assert !iterator.isReverseOrder();

        ArrayBackedPartition partition = new ArrayBackedPartition(iterator.metadata(),
                                                                  iterator.partitionKey(),
                                                                  iterator.partitionLevelDeletion(),
                                                                  iterator.columns(),
                                                                  initialRowCapacity);

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer();
        RangeTombstoneCollector markerCollector = partition.new RangeTombstoneCollector();

        try (AtomIterator iter = iterator)
        {
            while (iter.hasNext())
            {
                Atom atom = iter.next();
                if (atom.kind() == Atom.Kind.ROW)
                {
                    Rows.copy((Row)atom, writer);
                }
                else
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker)atom;
                    markerCollector.writeMarker(marker.clustering(), marker.isOpenMarker(), marker.delTime());
                }
            }
        }
        return partition;
    }

    public Atom tail()
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

    // The number of rows in this cached partition that have at least one
    // no-expiring non-deleted cell.
    public int rowsWithNonExpiringCells()
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
