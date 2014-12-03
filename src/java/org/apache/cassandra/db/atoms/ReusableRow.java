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

public class ReusableRow extends AbstractReusableRow
{
    private ClusteringPrefix clustering;
    private long timestamp = Long.MIN_VALUE;

    private final RowDataBlock data;
    private final Writer writer;

    public ReusableRow(Columns columns)
    {
        this.data = new RowDataBlock(columns, 1);
        this.writer = new Writer(data);
    }

    protected RowDataBlock data()
    {
        return data;
    }

    protected int row()
    {
        return 0;
    }

    public ClusteringPrefix clustering()
    {
        assert clustering != null;
        return clustering;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public Row.Writer writer()
    {
        return writer.reset();
    }

    private class Writer extends RowDataBlock.Writer
    {
        public Writer(RowDataBlock data)
        {
            super(data);
        }

        public void writeClustering(ClusteringPrefix clustering)
        {
            ReusableRow.this.clustering = clustering.takeAlias();
        }

        public void writeTimestamp(long timestamp)
        {
            ReusableRow.this.timestamp = timestamp;
        }
    }
}
