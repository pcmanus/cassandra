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

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASConditions;

/**
 * CAS conditions on potentially multiple rows of the same partition.
 */
public class CQL3CasConditions implements CASConditions
{
    private final CFMetaData cfm;
    private final ByteBuffer key;
    private final long now;

    // We index RowCondition by the prefix of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<ByteBuffer, RowCondition> conditions;

    public CQL3CasConditions(CFMetaData cfm, ByteBuffer key, long now)
    {
        this.cfm = cfm;
        this.key = key;
        this.now = now;
        this.conditions = new TreeMap<ByteBuffer, RowCondition>(cfm.comparator);
    }

    public void addNotExist(ColumnNameBuilder prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix.build(), new NotExistCondition(prefix, now));
        if (previous != null && !(previous instanceof NotExistCondition))
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
    }

    public void addConditions(ColumnNameBuilder prefix, Collection<Operation> operations, List<ByteBuffer> variables) throws InvalidRequestException
    {
        ByteBuffer b = prefix.build();
        RowCondition condition = conditions.get(b);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix, cfm, now);
            conditions.put(b, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(operations, key, variables);
    }

    public IDiskAtomFilter readFilter()
    {
        assert !conditions.isEmpty();
        ColumnSlice[] slices = new ColumnSlice[conditions.size()];
        int i = 0;
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values on why there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Map.Entry<ByteBuffer, RowCondition> entry : conditions.entrySet())
            slices[i++] = new ColumnSlice(entry.getKey(), entry.getValue().rowPrefix.buildAsEndOfRange());

        return new SliceQueryFilter(slices, false, slices.length, cfm.clusteringKeyColumns().size());
    }

    public boolean appliesTo(ColumnFamily current)
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    private static abstract class RowCondition
    {
        public final ColumnNameBuilder rowPrefix;
        protected final long now;

        protected RowCondition(ColumnNameBuilder rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public abstract boolean appliesTo(ColumnFamily current);
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(ColumnNameBuilder rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return true;

            Iterator<Column> iter = current.iterator(new ColumnSlice[]{ new ColumnSlice(rowPrefix.build(), rowPrefix.buildAsEndOfRange()) });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return false;
            return true;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final CFMetaData cfm;
        private final ColumnFamily expected;

        private ColumnsConditions(ColumnNameBuilder rowPrefix, CFMetaData cfm, long now)
        {
            super(rowPrefix, now);
            this.cfm = cfm;
            this.expected = TreeMapBackedSortedColumns.factory.create(cfm);
        }

        public void addConditions(Collection<Operation> conditions, ByteBuffer key, List<ByteBuffer> variables) throws InvalidRequestException
        {
            // When building the conditions, we should not use a TTL. It's not useful, and if a very low ttl (1 seconds) is used, it's possible
            // for it to expire before the actual build of the conditions which would break since we would then testing for the presence of tombstones.
            UpdateParameters params = new UpdateParameters(cfm, variables, now, 0, null);

            for (Operation condition : conditions)
                condition.execute(key, expected, rowPrefix.copy(), params);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return false;

            for (Column e : expected)
            {
                Column c = current.getColumn(e.name());
                if (e.isLive(now))
                {
                    if (c == null || !c.isLive(now) || !c.value().equals(e.value()))
                        return false;
                }
                else
                {
                    // If we have a tombstone in expected, it means the condition tests that the column is
                    // null, so check that we have no value
                    if (c != null && c.isLive(now))
                        return false;
                }
            }
            return true;
        }

        @Override
        public String toString()
        {
            return expected.toString();
        }
    }
}
