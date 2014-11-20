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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableBufferAllocator extends MemtableAllocator
{

    protected MemtableBufferAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        super(onHeap, offHeap);
    }

    public RowAllocator newRowAllocator(CFMetaData cfm, OpOrder.Group writeOp)
    {
        if (cfm.isCounter())
            throw new UnsupportedOperationException();

        return new RowBufferAllocator(allocator(writeOp));
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new BufferDecoratedKey(key.getToken(), allocator(writeOp).clone(key.getKey()));
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    protected AbstractAllocator allocator(OpOrder.Group writeOp)
    {
        return new ContextAllocator(writeOp, this);
    }

    private static class RowBufferAllocator extends RowDataBlock.Writer implements RowAllocator
    {
        private final AbstractAllocator allocator;

        private MemtableRow.BufferClusteringPrefix clustering;
        private long rowTimestamp;
        private RowDataBlock data;

        private RowBufferAllocator(AbstractAllocator allocator)
        {
            this.allocator = allocator;
        }

        public void allocateNewRow(Columns columns)
        {
            data = new RowDataBlock(columns, 1);
            updateWriter(data);
        }

        public MemtableRow allocatedRow()
        {
            MemtableRow row = new MemtableRow.BufferRow(clustering, rowTimestamp, data);

            clustering = null;
            rowTimestamp = Rows.NO_TIMESTAMP;
            data = null;

            return row;
        }

        public void setClustering(ClusteringPrefix clustering)
        {
            clustering = MemtableRow.BufferClusteringPrefix.clone(clustering, allocator);
        }

        public void setTimestamp(long timestamp)
        {
            rowTimestamp = timestamp;
        }

        @Override
        public void addCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            ByteBuffer v = allocator.clone(value);
            if (column.isComplex())
                complexWriter.addCell(column, v, timestamp, localDeletionTime, ttl, MemtableRow.BufferCellPath.clone(path, allocator));
            else
                simpleWriter.addCell(column, v, timestamp, localDeletionTime, ttl);
        }
    }
}
