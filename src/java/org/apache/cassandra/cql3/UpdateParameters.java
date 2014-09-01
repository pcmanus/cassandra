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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A simple container that simplify passing parameters for collections methods.
 */
public class UpdateParameters
{
    public final CFMetaData metadata;
    public final QueryOptions options;
    public final long timestamp;
    private final int ttl;
    public final int localDeletionTime;

    // For lists operation that require a read-before-write. Will be null otherwise.
    private final Map<ByteBuffer, Map<ClusteringPrefix, Row>> prefetchedLists;

    public UpdateParameters(CFMetaData metadata, QueryOptions options, long timestamp, int ttl, Map<ByteBuffer, Map<ClusteringPrefix, Row>> prefetchedLists)
    {
        this.metadata = metadata;
        this.options = options;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = (int)(System.currentTimeMillis() / 1000);
        this.prefetchedLists = prefetchedLists;
    }

    public Cell makeCell(ByteBuffer value) throws InvalidRequestException
    {
        return Cells.create(value, timestamp, ttl, localDeletionTime, metadata);
    }

    public Cell makeCell(CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        return Cells.create(path, value, timestamp, ttl, localDeletionTime, metadata);
    }

     public Cell makeCounter(long delta) throws InvalidRequestException
     {
         return Cells.createCounterUpdate(delta, FBUtilities.timestampMicros());
     }

    public Cell makeTombstone() throws InvalidRequestException
    {
        return Cells.createTombsone(localDeletionTime, timestamp);
    }

    public Cell makeTombstone(CellPath path) throws InvalidRequestException
    {
        return Cells.createTombsone(path, localDeletionTime, timestamp);
    }

    public DeletionTime deletionTime()
    {
        return new SimpleDeletionTime(timestamp, localDeletionTime);
    }

    public DeletionTime complexDeletionTime()
    {
        return new SimpleDeletionTime(timestamp, localDeletionTime);
    }

    public DeletionTime complexDeletionTimeForOverwrite()
    {
        return new SimpleDeletionTime(timestamp-1, localDeletionTime);
    }

    public RangeTombstone makeRangeTombstone(ClusteringPrefix prefix)
    {
        return new RangeTombstone(prefix.withEOC(ClusteringPrefix.EOC.START), prefix.withEOC(ClusteringPrefix.EOC.END), deletionTime());
    }

    public ColumnData getPrefetchedList(ByteBuffer rowKey, ClusteringPrefix clustering, ColumnDefinition c)
    {
        if (prefetchedLists == null)
            return null;

        Map<ClusteringPrefix, Row> m = prefetchedLists.get(rowKey);
        if (m == null)
            return null;

        Row row = m.get(clustering);
        return row == null ?  null : row.data(c);
    }
}
