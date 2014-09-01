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
import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * Storage engine representation of a row.
 *
 * A row is identified by it's clustering column values (it's an Atom). It
 * has a row level timestamp and contains data regarding the columns it
 * contains.
 */
public interface Row extends Atom, Iterable<ColumnData>
{
    /**
     * The row timestamp.
     *
     * This correspond to the timestamp of the last INSERT done on the row. A
     * row with no column data but with a timestamp() > Long.MIN_VALUE is still
     * considered live (from the CQL standpoint, it exists but has all it's
     * non-PK column null).
     */
    public long timestamp();

    /**
     * Whether the row has no information whatsoever. This means no timestamp and
     * no column data.
     */
    public boolean isEmpty();

    /**
     * Returns the data for column c.
     *
     * Calls to this method are allowed to return the same ColumnData object, and hence the returned
     * object is only valid until the next get() call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the ColumnData object
     * longer than that.
     *
     * Returns null if the row has no data for this column.
     */
    public ColumnData data(ColumnDefinition c);

    /**
     * An iterator over the data of this row.
     *
     * The iterator guarantees that for 2 rows of the same partition, columns
     * are returned in a consistent order in the sense that if the data for
     * column c1 is returned before the data for column c2 by the first iterator,
     * it is also the case for the 2nd iterator.
     *
     * The object returned by a call to next() is only guaranteed to be valid until
     * the next call to hasNext() or next(). If a consumer wants to keep a
     * reference on the returned Cell objects for longer than the iteration, it must
     * make a copy of it explicitly.
     */
    public Iterator<ColumnData> iterator();

    @Override
    public Row takeAlias();
}
