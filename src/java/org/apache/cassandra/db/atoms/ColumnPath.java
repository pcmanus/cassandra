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

import java.util.Objects;

import org.apache.cassandra.db.*;

public class ColumnPath
{
    public final DecoratedKey key;
    public final ClusteringPrefix clustering;
    public final ColumnDefinition column;

    public ColumnPath(DecoratedKey key, ClusteringPrefix clustering, ColumnDefinition column)
    {
        this.key = key;
        this.clustering = clustering.takeAlias();
        this.column = column;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(key, clustering, column);
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ColumnPath))
            return false;

        ColumnPath that = (ColumnPath)o;
        return Objects.equal(this.key, that.key)
            && Objects.equal(this.clustering, that.clustering)
            && Objects.equal(this.column, that.column);
    }
}
