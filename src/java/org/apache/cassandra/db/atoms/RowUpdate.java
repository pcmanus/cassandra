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

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.concurrent.OpOrder;

public interface RowUpdate extends Row
{
    public Columns columns();

    public RowUpdate mergeTo(RowUpdate other, SecondaryIndexManager.Updater indexUpdater);

    public RowUpdate setClustering(ClusteringPrefix clustering);
    public RowUpdate updateRowTimestamp(long timestamp);
    public RowUpdate updateComplexDeletion(ColumnDefinition c, DeletionTime time);
    public RowUpdate addCell(ColumnDefinition column, Cell cell);

    public RowUpdate localCopy(CFMetaData metaData, MemtableAllocator allocator, OpOrder.Group opGroup);
    public int dataSize();
    // returns the size of the Cell and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    public long unsharedHeapSizeExcludingData();
}
