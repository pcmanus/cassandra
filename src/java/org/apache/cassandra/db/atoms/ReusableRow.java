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

import org.apache.cassandra.db.*;

public class ReusableRow extends ColumnDataContainer.Reader
{
    private ClusteringPrefix clustering;
    private long rowTimestamp = Long.MIN_VALUE;

    public ReusableRow(Columns columns, int cellsCapacity)
    {
        super(new ColumnDataContainer(columns, 1, cellsCapacity));
    }

    public ReusableRow(Columns columns)
    {
        this(columns, 16);
    }

    public long timestamp()
    {
        return rowTimestamp;
    }

    public ClusteringPrefix clustering()
    {
        return clustering;
    }

    protected int rowOffset()
    {
        return 0;
    }

    public Writer newWriter()
    {
        return new Writer();
    }

    public Row takeAlias()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public class Writer extends ColumnDataContainer.Writer
    {
        private Writer()
        {
            super(data());
        }

        protected int startPosition()
        {
            return 0;
        }

        protected int rowOffset()
        {
            return 0;
        }

        public void setClustering(ClusteringPrefix clustering)
        {
            ReusableRow.this.clustering = clustering;
        }

        public void setTimestamp(long rowTimestamp)
        {
            ReusableRow.this.rowTimestamp = rowTimestamp;
        }
    }
}
