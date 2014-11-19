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

import java.nio.ByteBuffer;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Rows stored inside a memtable.
 *
 * This has methods like dataSize and unsharedHeapSizeExcludingData that are
 * specific to memtables.
 */
public interface MemtableRow extends Row
{
    public int dataSize();

    // returns the size of the Row and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    public long unsharedHeapSizeExcludingData();

    public class BufferRow extends AbstractReusableRow implements IMeasurableMemory
    {
        private final BufferClusteringPrefix clustering;
        private final long timestamp;

        public BufferRow(BufferClusteringPrefix clustering, long timestamp, RowDataBlock data)
        {
            super(data);
            this.clustering = clustering;
            this.timestamp = timestamp;
        }

        public ClusteringPrefix clustering()
        {
            return clustering;
        }

        public long timestamp()
        {
            return rowTimestamp;
        }

        public int dataSize()
        {
            throw new UnsupportedOperationException();
        }

        public long unsharedHeapSizeExcludingData()
        {
            throw new UnsupportedOperationException();
        }
    }

    public class BufferClusteringPrefix extends AbstractClusteringPrefix
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferClusteringPrefix(new ByteBuffer[0]));

        private final ByteBuffer[] values;

        private BufferClusteringPrefix(ByteBuffer[] values)
        {
            this.values = values;
        }

        public BufferClusteringPrefix clone(ClusteringPrefix clustering, AbstractAllocator allocator)
        {
            // We're currently only using this for rows, which don't have a EOC
            assert clustering.eoc() == EOC.NONE;
            ByteBuffer[] values = new ByteBuffer[clustering.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = allocator.clone(clustering.get(i));
            return new BufferClusteringPrefix(values);
        }

        public int size()
        {
            return values.length;
        }

        public ByteBuffer get(int i)
        {
            return values[i];
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
        }

        public ClusteringPrefix takeAlias()
        {
            return this;
        }
    }

    public class BufferCellPath implements CellPath
    {
    }
}
