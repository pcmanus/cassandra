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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filters.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public abstract class SecondaryIndexSearcher
{
    protected final SecondaryIndexManager indexManager;
    protected final Set<ColumnDefinition> columns;
    protected final ColumnFamilyStore baseCfs;

    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }

    public SecondaryIndex highestSelectivityIndex(ColumnFilter filter)
    {
        ColumnFilter.Expression expr = highestSelectivityPredicate(filter);
        return expr == null ? null : indexManager.getIndexForColumn(expr.column());
    }

    public abstract PartitionIterator search(ReadCommand command);

    /**
     * @return true this index is able to handle the given index expressions.
     */
    public boolean canHandle(ColumnFilter filter)
    {
        for (ColumnFilter.Expression expression : filter)
        {
            if (!columns.contains(expression.column()) || !expression.operator().allowsIndexQuery())
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column());
            if (index != null && index.getIndexCfs() != null)
                return true;
        }
        return false;
    }
    
    /**
     * Validates the specified {@link IndexExpression}. It will throw an {@link org.apache.cassandra.exceptions.InvalidRequestException}
     * if the provided clause is not valid for the index implementation.
     *
     * @param indexExpression An {@link IndexExpression} to be validated
     * @throws org.apache.cassandra.exceptions.InvalidRequestException in case of validation errors
     */
    public void validate(ColumnFilter.Expression expression) throws InvalidRequestException
    {
    }

    protected ColumnFilter.Expression highestSelectivityPredicate(ColumnFilter filter)
    {
        ColumnFilter.Expression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        Map<SecondaryIndex, Integer> candidates = new HashMap<>();

        for (ColumnFilter.Expression expression : filter)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column()))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column());
            if (index == null || index.getIndexCfs() == null || !expression.operator().allowsIndexQuery())
                continue;
            int meanColumns = index.getIndexCfs().getMeanColumns();
            candidates.put(index, meanColumns);
            if (meanColumns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = meanColumns;
            }
        }

        if (best == null)
            Tracing.trace("No applicable indexes found");
        else
            Tracing.trace("Candidate index mean cardinalities are {}. Scanning with {}.",
                          FBUtilities.toString(candidates), indexManager.getIndexForColumn(best.column()).getIndexName());

        return best;
    }
}
