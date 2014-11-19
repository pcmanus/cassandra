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

    private Writer writer = new Writer();

    public ReusableRow(Columns columns)
    {
        super(new RowDataBlock(columns, 1));
    }

    public ClusteringPrefix clustering()
    {
        assert clustering != null;
        return clustering;
    }

    public long timestamp()
    {
        return rowTimestamp;
    }

    public Rows.Writer writer()
    {
        return writer.reset();
    }

    private class Writer extends RowDataBlock.Writer
    {
        public Writer()
        {
            super(data);
        }

        public void setClustering(ClusteringPrefix clustering)
        {
            ReusableRow.this.clustering = clustering.takeAlias();
        }

        public void setTimestamp(long timestamp)
        {
            ReusableRow.this.timestamp = timestamp;
        }
    }
}
