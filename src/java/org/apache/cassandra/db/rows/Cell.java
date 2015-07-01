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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Comparator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A cell is our atomic unit for a single value of a single column.
 * <p>
 * A cell always holds at least a timestamp that gives us how the cell reconcile. We then
 * have 3 main types of cells:
 *   1) live regular cells: those will also have a value and, if for a complex column, a path.
 *   2) expiring cells: on top of regular cells, those have a ttl and a local deletion time (when they are expired).
 *   3) tombstone cells: those won't have value, but they have a local deletion time (when the tombstone was created).
 */
public interface Cell extends ColumnData
{
    public final Comparator<Cell> comparator = new Comparator<Cell>()
    {
        public int compare(Cell c1, Cell c2)
        {
            int cmp = c1.column().compareTo(c2.column());
            if (cmp != 0)
                return cmp;

            Comparator<CellPath> pathComparator = c1.column().cellPathComparator();
            return pathComparator == null ? 0 : pathComparator.compare(c1.path(), c2.path());
        }
    };

    public final Serializer serializer = new BufferCell.Serializer();

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    public boolean isCounterCell();

    /**
     * The cell value.
     *
     * @return the cell value.
     */
    public ByteBuffer value();

    /**
     * The liveness info of the cell, that is its timestamp and whether it is
     * expiring, deleted or none of the above.
     *
     * @return the cell {@link LivenessInfo}.
     */
    public LivenessInfo livenessInfo();

    /**
     * The cell timestamp.
     * <p>
     * This is a shortcut for {@code livenessInfo().timestamp()}.
     *
     * @return the cell timestamp.
     */
    public long timestamp();

    /**
     * The cell ttl.
     * <p>
     * This is a shortcut for {@code livenessInfo().ttl()}.
     *
     * @return the cell ttl.
     */
    public int ttl();

    /**
     * The cell local deletion time.
     * <p>
     * This is a shortcut for {@code livenessInfo().localDeletionTime()}.
     *
     * @return the cell local deletion time.
     */
    public int localDeletionTime();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    public boolean isTombstone();

    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    public boolean isExpiring();

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    public boolean isLive(int nowInSec);

    /**
     * For cells belonging to complex types (non-frozen collection and UDT), the
     * path to the cell.
     *
     * @return the cell path for cells of complex column, and {@code null} for other cells.
     */
    public CellPath path();

    /**
     * Returns a copy of this cell but with the updated provided timestamp.
     *
     * @return a copy of this cell but with {@code timestamp} as timestamp.
     */
    public Cell withUpdatedTimestamp(long timestamp);

    public Cell withUpdatedValue(ByteBuffer newValue);

    public Cell copy(AbstractAllocator allocator);

    @Override
    // Overrides super type to provide a more precise return type.
    public Cell markCounterLocalToBeCleared();

    @Override
    // Overrides super type to provide a more precise return type.
    public Cell purge(DeletionPurger purger, int nowInSec);

    public interface Serializer
    {
        public void serialize(Cell cell, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException;

        public Cell deserialize(DataInputPlus in, LivenessInfo rowLiveness, ColumnDefinition column, SerializationHeader header, SerializationHelper helper) throws IOException;

        public long serializedSize(Cell cell, LivenessInfo rowLiveness, SerializationHeader header);

        // Returns if the skipped cell was an actual cell (i.e. it had its presence flag).
        public boolean skip(DataInputPlus in, ColumnDefinition column, SerializationHeader header) throws IOException;
    }
}
