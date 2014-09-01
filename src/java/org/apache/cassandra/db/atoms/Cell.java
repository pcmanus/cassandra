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

import org.apache.cassandra.db.Aliasable;

/**
 * A cell holds a single "simple" value for a given column.
 *
 * A composite collection (collection or non-frozen UDTs) are comprised of multiple cells.
 */
public interface Cell extends Aliasable<Cell>
{
    public boolean isCounterCell();

    public ByteBuffer value();

    public long timestamp();
    public int localDeletionTime();

    public int ttl();

    //public long timestampOfLastDelete(); // For counters

    /**
     * For cell belonging to complex types (collection and UDT values), the path to the cell.
     *
     * Returns {@code null} for cell belonging to simple types.
     */
    public CellPath path();
}
