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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.partitions.ReadPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.utils.Pair;

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
public class CQL3CasRequest implements CASRequest
{
    private final CFMetaData cfm;
    private final DecoratedKey key;
    private final boolean isBatch;

    // We index RowCondition by the prefix of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<ClusteringPrefix, RowCondition> conditions;

    private final List<RowUpdate> updates = new ArrayList<>();

    public CQL3CasRequest(CFMetaData cfm, DecoratedKey key, boolean isBatch)
    {
        this.cfm = cfm;
        this.key = key;
        this.conditions = new TreeMap<>(cfm.comparator);
        this.isBatch = isBatch;
    }

    public void addRowUpdate(ClusteringPrefix prefix, ModificationStatement stmt, QueryOptions options, long timestamp)
    {
        updates.add(new RowUpdate(prefix, stmt, options, timestamp));
    }

    public void addNotExist(ClusteringPrefix prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new NotExistCondition(prefix));
        if (previous != null && !(previous instanceof NotExistCondition))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            if (previous instanceof ExistCondition)
                throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            else
                throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
    }

    public void addExist(ClusteringPrefix prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new ExistCondition(prefix));
        // this should be prevented by the parser, but it doesn't hurt to check
        if (previous instanceof NotExistCondition)
            throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
    }

    public void addConditions(ClusteringPrefix prefix, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = conditions.get(prefix);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix);
            conditions.put(prefix, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    public PartitionFilter readFilter()
    {
        assert !conditions.isEmpty();
        Slices.Builder builder = new Slices.Builder(cfm.comparator, conditions.size());
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values for which there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (ClusteringPrefix prefix : conditions.keySet())
            builder.add(prefix.withEOC(ClusteringPrefix.EOC.START), prefix.withEOC(ClusteringPrefix.EOC.END));

        // TODO: we should actually collect the columns on which we have conditions.
        return new SlicePartitionFilter(cfm.regularColumns(), cfm.staticColumns(), builder.build(), false);
    }

    public boolean appliesTo(ReadPartition current) throws InvalidRequestException
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    public PartitionUpdate makeUpdates(ReadPartition current) throws InvalidRequestException
    {
        PartitionUpdate update = new PartitionUpdate(cfm, key);
        for (RowUpdate upd : updates)
            upd.applyUpdates(current, update);

        if (isBatch)
            BatchStatement.verifyBatchSize(Collections.singleton(update));

        return update;
    }

    /**
     * Due to some operation on lists, we can't generate the update that a given Modification statement does before
     * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
     * (include the statement iself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
     * we'll have only one.
     */
    private class RowUpdate
    {
        private final ClusteringPrefix rowPrefix;
        private final ModificationStatement stmt;
        private final QueryOptions options;
        private final long timestamp;

        private RowUpdate(ClusteringPrefix rowPrefix, ModificationStatement stmt, QueryOptions options, long timestamp)
        {
            this.rowPrefix = rowPrefix.takeAlias();
            this.stmt = stmt;
            this.options = options;
            this.timestamp = timestamp;
        }

        public void applyUpdates(ReadPartition current, PartitionUpdate updates) throws InvalidRequestException
        {
            Map<ByteBuffer, Map<ClusteringPrefix, Row>> map = null;
            if (stmt.requiresRead())
            {
                Row row = current.getRow(rowPrefix);
                if (row != null)
                    map = Collections.singletonMap(key.getKey(), Collections.singletonMap(rowPrefix, row.takeAlias()));
            }

            UpdateParameters params = new UpdateParameters(cfm, options, timestamp, stmt.getTimeToLive(options), map);
            stmt.addUpdateForKey(updates, rowPrefix, params);
        }
    }

    private static abstract class RowCondition
    {
        public final ClusteringPrefix rowPrefix;

        protected RowCondition(ClusteringPrefix rowPrefix)
        {
            this.rowPrefix = rowPrefix;
        }

        public abstract boolean appliesTo(ReadPartition current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(ClusteringPrefix rowPrefix)
        {
            super(rowPrefix);
        }

        public boolean appliesTo(ReadPartition current)
        {
            return current == null || current.getRow(rowPrefix) == null;
        }
    }

    private static class ExistCondition extends RowCondition
    {
        private ExistCondition(ClusteringPrefix rowPrefix)
        {
            super(rowPrefix);
        }

        public boolean appliesTo(ReadPartition current)
        {
            return current != null && current.getRow(rowPrefix) != null;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Multimap<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions = HashMultimap.create();

        private ColumnsConditions(ClusteringPrefix rowPrefix)
        {
            super(rowPrefix);
        }

        public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                ColumnCondition.Bound current = condition.bind(options);
                conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
            }
        }

        public boolean appliesTo(ReadPartition current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition.Bound condition : conditions.values())
            {
                if (!condition.appliesTo(current.getRow(rowPrefix)))
                    return false;
            }
            return true;
        }
    }
}
