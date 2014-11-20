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

import java.io.Closeable;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * An iterator over atoms belonging to a partition.
 *
 * Any implementation of AtomIterator *must* provide the following guarantees:
 *   1) the returned atoms must in clustering order, or reverse clustering order
 *      iff isReversedOrder() is true.
 *   2) the partition level deletion and range tombstone marker (rows and cells ones)
 *      returned must *not* delete any cells of the rows returned by the iterator.
 *      Concretely, this means that only the merging of multiple iterators have to
 *      worry about deletion.
 *
 * Note further that the objects returned by next() are only valid until the
 * next call to hasNext() or next(). If a consumer wants to keep a reference on
 * the returned objects for longer than the iteration, it must make a copy of
 * it explicitly.
 */
public interface AtomIterator extends Iterator<Atom>, Closeable
{
    /**
     * The metadata for the table this iterator on.
     */
    public CFMetaData metadata();

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public PartitionColumns columns();

    /**
     * Whether or not the atom returned by this iterator are in reversed
     * clustering order.
     */
    public boolean isReverseOrder();

    /**
     * The partition key of the partition this in an iterator over.
     */
    public DecoratedKey partitionKey();

    /**
     * The partition level deletion for the partition this iterate over.
     */
    public DeletionTime partitionLevelDeletion();

    /**
     * The static part corresponding to this partition (this can be an empty
     * row).
     */
    public Row staticRow();
}
