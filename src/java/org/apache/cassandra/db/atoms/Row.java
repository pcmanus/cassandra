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
import org.apache.cassandra.db.Aliasable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;

/**
 * Storage engine representation of a row.
 *
 * A row is identified by it's clustering column values (it's an Atom). It
 * has a row level timestamp and contains data regarding the columns it
 * contains.
 */
public interface Row extends Atom, Iterable<Cell>, Aliasable<Row>
{
    /**
     * The columns this row contains.
     *
     * Note that this is actually a superset of the columns the row contains. The row
     * may not have values for each of those columns, but it can't have values for other
     * columns.
     *
     * @return a superset of the columns contained in this row.
     */
    public Columns columns();

    /**
     * The row timestamp.
     *
     * This correspond to the timestamp of the last INSERT done on the row. A
     * row with no column data but with a {@code timestamp() > Long.MIN_VALUE} is
     * still considered live (from the CQL standpoint, it exists but has all
     * it's non-PK column null).
     *
     * @return the timestamp for the row.
     */
    public long timestamp();

    /**
     * Whether the row has no information whatsoever. This means no timestamp,
     * no cells and no complex deletion info.
     *
     * @return {@code true} if the row has no data whatsoever, {@code false} otherwise.
     */
    public boolean isEmpty();

    /**
     * Returns the cell for simple column c.
     *
     * Calls to this method are allowed to return the same Cell object, and hence the returned
     * object is only valid until the next getCell() call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the Cell object
     * longer than that.
     *
     * @param c the simple column for which to fetch the cell.
     * @return {@code null} if the row has no cell for this column or if {@code c} is a
     * complex column (use getCells for complex columns).
     */
    public Cell getCell(ColumnDefinition c);

    /**
     * Returns an iterator on the cells of a complex column c.
     *
     * Calls to this method are allowed to return the same iterator object, and
     * hence the returned object is only valid until the next getCells() call
     * on the same Row object. You will need to copy the returned data if you
     * plan on using a reference to the Cell object longer than that.
     *
     * @param c the simple column for which to fetch the cell.
     * @return {@code null} if the row has no cell for this column or if {@code c} is a
     * complex column (use getCells for complex columns).
     */
    public Iterator<Cell> getCells(ColumnDefinition c);

    /**
     * Deletion informations for complex columns.
     *
     * @param c the complex column for which to fetch deletion info.
     * @return the deletion time for complex column {@code c} in this row.
     */
    public DeletionTime getDeletion(ColumnDefinition c);

    /**
     * An iterator over the cells of this row.
     *
     * The iterator guarantees that for 2 rows of the same partition, columns
     * are returned in a consistent order in the sense that if the cells for
     * column c1 is returned before the cells for column c2 by the first iterator,
     * it is also the case for the 2nd iterator.
     *
     * The object returned by a call to next() is only guaranteed to be valid until
     * the next call to hasNext() or next(). If a consumer wants to keep a
     * reference on the returned Cell objects for longer than the iteration, it must
     * make a copy of it explicitly.
     *
     * @return an iterator over the cells of this row.
     */
    public Iterator<Cell> iterator();
}
