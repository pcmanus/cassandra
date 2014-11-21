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
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
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
    private final Map<DecoratedKey, Partition> prefetchedRows;

    public UpdateParameters(CFMetaData metadata, QueryOptions options, long timestamp, int ttl, Map<DecoratedKey, Partition> prefetchedRows)
    {
        this.metadata = metadata;
        this.options = options;
        this.timestamp = timestamp;
        this.ttl = ttl > 0 ? ttl : metadata.getDefaultTimeToLive();
        this.localDeletionTime = FBUtilities.nowInSeconds();
        this.prefetchedRows = prefetchedRows;
    }

    public void addTombstone(ColumnDefinition column, Row.Writer writer) throws InvalidRequestException
    {
        addTombstone(column, writer, null);
    }

    public void addTombstone(ColumnDefinition column, Row.Writer writer, CellPath path) throws InvalidRequestException
    {
        writer.writeCell(column, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, localDeletionTime, Cells.NO_TTL, path);
    }

    public void addCell(ColumnDefinition column, Row.Writer writer, ByteBuffer value) throws InvalidRequestException
    {
        addCell(column, writer, null, value);
    }

    public void addCell(ColumnDefinition column, Row.Writer writer, CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        writer.writeCell(column, false, value, timestamp, localDeletionTime, ttl, null);
    }

    public void addCounter(ColumnDefinition column, Row.Writer writer, long increment) throws InvalidRequestException
    {
        writer.writeCell(column, true, ByteBufferUtil.bytes(increment), timestamp, localDeletionTime, Cells.NO_TTL, null);
    }

    public void setComplexDeletionTime(ColumnDefinition column, Row.Writer writer)
    {
        writer.writeComplexDeletion(column, deletionTime());
    }

    public void setComplexDeletionTimeForOverwrite(ColumnDefinition column, Row.Writer writer)
    {
        writer.writeComplexDeletion(column, new SimpleDeletionTime(timestamp - 1, localDeletionTime));
    }

    public DeletionTime deletionTime()
    {
        return new SimpleDeletionTime(timestamp, localDeletionTime);
    }

    public RangeTombstone makeRangeTombstone(ClusteringPrefix prefix)
    {
        return new RangeTombstone(prefix.withEOC(ClusteringPrefix.EOC.START), prefix.withEOC(ClusteringPrefix.EOC.END), deletionTime());
    }

    public Row getPrefetchedRow(ByteBuffer key, ClusteringPrefix clustering)
    {
        if (prefetchedRows == null)
            return null;

        Partition partition = prefetchedRows.get(key);
        return partition == null ? null : partition.searchIterator().next(clustering);
    }
}
