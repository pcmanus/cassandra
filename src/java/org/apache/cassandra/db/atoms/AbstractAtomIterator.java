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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

public abstract class AbstractAtomIterator extends AbstractIterator<Atom> implements AtomIterator
{
    protected final CFMetaData metadata;
    protected final DecoratedKey partitionKey;
    protected final DeletionTime partitionLevelDeletion;
    protected final PartitionColumns columns;
    protected final Row staticRow;
    protected final boolean isReverseOrder;

    protected AbstractAtomIterator(CFMetaData metadata,
                                   DecoratedKey partitionKey,
                                   DeletionTime partitionLevelDeletion,
                                   PartitionColumns columns,
                                   Row staticRow,
                                   boolean isReverseOrder)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.columns = columns;
        this.staticRow = staticRow;
        this.isReverseOrder = isReverseOrder;
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

    public void close()
    {
    }
}

