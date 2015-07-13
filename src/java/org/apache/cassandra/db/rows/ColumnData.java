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

import java.security.MessageDigest;
import java.util.Comparator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DeletionPurger;

/**
 * Generic interface for the data of a given column (inside a row).
 *
 * In practice, there is only 2 implementations of this: either {@link Cell} for simple columns
 * or {@code ComplexColumnData} for complex columns.
 */
public interface ColumnData
{
    public static final Comparator<ColumnData> comparator = (cd1, cd2) -> cd1.column().compareTo(cd2.column());

    // A comparator for the cells of the *similar* ColumnData, i.e. one that assumes the cells are all for the same column.
    public static final Comparator<Cell> cellComparator = (c1, c2) -> c1.column().cellPathComparator().compare(c1.path(), c2.path());

    /**
     * The column this is data for.
     *
     * @return the column this is a data for.
     */
    public ColumnDefinition column();

    /**
     * The size of the data hold by this {@code ColumnData}.
     *
     * @return the size used by the data of this {@code ColumnData}.
     */
    public int dataSize();

    public long unsharedHeapSizeExcludingData();

    /**
     * Validate the column data.
     *
     * @throws MarshalException if the data is not valid.
     */
    public void validate();

    /**
     * Adds the data to the provided digest.
     *
     * @param digest the {@code MessageDigest} to add the data to.
     */
    public void digest(MessageDigest digest);

    /**
     * Returns a copy of the data where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     *
     * This exists for the Paxos path, see {@link PartitionUpdate#updateAllTimestamp} for additional details.
     */
    public ColumnData updateAllTimestamp(long newTimestamp);

    public ColumnData markCounterLocalToBeCleared();

    public ColumnData purge(DeletionPurger purger, int nowInSec);
}
