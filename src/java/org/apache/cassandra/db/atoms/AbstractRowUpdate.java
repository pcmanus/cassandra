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

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;

public abstract class AbstractRowUpdate implements RowUpdate
{
    protected ClusteringPrefix clustering;
    protected long rowTimestamp = Long.MIN_VALUE;

    public RowUpdate setClustering(ClusteringPrefix clustering)
    {
        this.clustering = clustering.takeAlias();
        return this;
    }

    public Row takeAlias()
    {
        return this;
    }

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    public ClusteringPrefix clustering()
    {
        return clustering;
    }

    public long timestamp()
    {
        return rowTimestamp;
    }

    public RowUpdate updateRowTimestamp(long timestamp)
    {
        rowTimestamp = timestamp;
        return this;
    }
}
