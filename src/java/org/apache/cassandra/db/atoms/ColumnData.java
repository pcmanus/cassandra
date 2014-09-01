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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Aliasable;
import org.apache.cassandra.db.DeletionTime;

/**
 * Holds data on a given column of a particular row.
 */
public interface ColumnData
{
    /**
     * The column this is the data of.
     */
    public ColumnDefinition column();

    /**
     * The number of cells contained for this column.
     * Note that for non complex columns, this will always return 1 (it can't
     * return 0 since a non-complex column has no associated complexDeletionTime() and
     * so having no cell means having no data). This can be > 1 or even 0 for a complex
     * columns though.
     */
    public int size();

    /**
     * Returns the i-th cell of this column data (first cell is 0), where i must be between
     * 0 and size() - 1. Note that the returned Cell object is only valid until the next call
     * to this method and callers should copy() the cell if they need it for longer than that.
     */
    public Cell cell(int i);

    /**
     * The deletion time associated to complex columns.
     * This allows to handle full collections deletion for instance.
     */
    public DeletionTime complexDeletionTime();
}
