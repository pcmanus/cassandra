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

package org.apache.cassandra.index;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class StubIndex implements Index
{
    public List<Row> rowsInserted = new ArrayList<>();
    public List<Row> rowsDeleted = new ArrayList<>();
    public List<Pair<Row,Row>> rowsUpdated = new ArrayList<>();
    private IndexMetadata indexMetadata;
    private ColumnFamilyStore baseCfs;

    public void reset()
    {
        rowsInserted.clear();
        rowsDeleted.clear();
    }

    public Callable<?> setIndexMetadata(IndexMetadata metadata)
    {
        indexMetadata = metadata;
        return null;
    }

    public boolean indexes(PartitionColumns columns)
    {
        for (ColumnDefinition col : columns)
            for (ColumnIdentifier indexed : indexMetadata.columns)
                if (indexed.equals(col.name))
                    return true;
        return false;
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        return operator == Operator.EQ;
    }

    public Optional<RowFilter> getReducedFilter(RowFilter filter)
    {
        return Optional.empty();
    }

    public Indexer indexerFor(DecoratedKey key,
                              int nowInSec,
                              OpOrder.Group opGroup,
                              SecondaryIndexManager.TransactionType transactionType)
    {
        return new Indexer()
        {
            public void insertRow(Row row)
            {
                rowsInserted.add(row);
            }

            public void removeRow(Row row)
            {
                rowsDeleted.add(row);
            }

            public void updateRow(Row oldRowData, Row newRowData)
            {
                rowsUpdated.add(Pair.create(oldRowData, newRowData));
            }
        };
    }

    public IndexMetadata getIndexMetadata()
    {
        return indexMetadata;
    }

    public String getIndexName()
    {
        return indexMetadata != null ? indexMetadata.name : "default_test_index_name";
    }

    public void init(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    public void register(IndexRegistry registry){
        registry.registerIndex(this);
    }

    public void maybeUnregister(IndexRegistry registry)
    {
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    public Collection<ColumnDefinition> getIndexedColumns()
    {
        return Collections.emptySet();
    }

    public Callable<?> getBlockingFlushTask()
    {
        return null;
    }

    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return null;
    }

    public Callable<?> getInvalidateTask()
    {
        return null;
    }

    public Callable<?> getMetadataReloadTask()
    {
        return null;
    }

    public long getEstimatedResultRows()
    {
        return 0;
    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {

    }

    public Searcher searcherFor(ReadCommand command)
    {
        return null;
    }

    public BiFunction<PartitionIterator, RowFilter, PartitionIterator> postProcessorFor(ReadCommand readCommand)
    {
        return null;
    }
}
