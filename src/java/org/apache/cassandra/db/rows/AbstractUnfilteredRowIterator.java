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
package org.apache.cassandra.db.rows;

import java.util.Objects;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

public abstract class AbstractUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    protected final CFMetaData metadata;
    protected final DecoratedKey partitionKey;
    protected final DeletionTime partitionLevelDeletion;
    protected final PartitionColumns columns;
    protected final Row staticRow;
    protected final boolean isReverseOrder;
    protected final RowStats stats;
    protected final int nowInSec;

    protected AbstractUnfilteredRowIterator(CFMetaData metadata,
                                            DecoratedKey partitionKey,
                                            DeletionTime partitionLevelDeletion,
                                            PartitionColumns columns,
                                            Row staticRow,
                                            boolean isReverseOrder,
                                            RowStats stats,
                                            int nowInSec)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.columns = columns;
        this.staticRow = staticRow;
        this.isReverseOrder = isReverseOrder;
        this.stats = stats;
        this.nowInSec = nowInSec;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public boolean isReverseOrder()
    {
        return isReverseOrder;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public RowStats stats()
    {
        return stats;
    }

    public int nowInSec()
    {
        return nowInSec;
    }

    public void close()
    {
    }

    public static boolean equal(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.columns(), b.columns())
            && Objects.equals(a.metadata(), b.metadata())
            && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
            && Objects.equals(a.partitionKey(), b.partitionKey())
            && Objects.equals(a.partitionLevelDeletion(), b.partitionLevelDeletion())
            && Objects.equals(a.staticRow(), b.staticRow())
            && Objects.equals(a.stats(), b.stats())
            && Objects.equals(a.metadata(), b.metadata())
            && Iterators.elementsEqual(a, b);
    }

}
