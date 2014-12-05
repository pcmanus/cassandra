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
package org.apache.cassandra.service.pager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.DataLimits;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    private final ConsistencyLevel consistencyLevel;
    private final boolean localQuery;

    protected final CFMetaData cfm;
    protected final DataLimits limits;

    private int remaining;
    private boolean exhausted;
    private boolean lastWasRecorded;

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 boolean localQuery,
                                 CFMetaData cfm,
                                 DataLimits limits)
    {
        this.consistencyLevel = consistencyLevel;
        this.localQuery = localQuery;

        this.cfm = cfm;
        this.limits = limits;

        this.remaining = limits.count();
    }

    public DataIterator fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return DataIterators.EMPTY;

        int currentPageSize = nextPageSize(pageSize);
        DataIterator iter = new PagerIterator(queryNextPage(currentPageSize, consistencyLevel, localQuery));
        return new CountingDataIterator(iter, limits.forPaging(pageSize))
        {
            @Override
            public void close()
            {
                super.close();
                remaining -= counter.counted();
                exhausted = counter.isDone();

            }
        };
    }

    private class PagerIterator extends WrappingDataIterator
    {
        private DecoratedKey lastKey;
        private Row lastRow;

        private PagerIterator(DataIterator iter)
        {
            super(iter);
        }

        @Override
        public RowIterator next()
        {
            RowIterator iter = super.next();
            lastKey = iter.partitionKey();
            return new RowPagerIterator(iter);
        }

        @Override
        public void close()
        {
            super.close();
            lastWasRecorded = lastRow == null ? false : recordLast(lastKey, lastRow);
        }

        private class RowPagerIterator extends WrappingRowIterator
        {
            private Row next;
            private boolean isFirst = true;

            RowPagerIterator(RowIterator iter)
            {
                super(iter);
            }

             @Override
            public boolean hasNext()
            {
                if (next != null)
                    return true;

                if (!super.hasNext())
                    return false;

                next = super.next();
                if (isFirst)
                {
                    isFirst = false;
                    if (shouldSkip(lastKey, next))
                        return hasNext();
                }
                return true;
            }

            @Override
            public Row next()
            {
                if (next == null && !hasNext())
                    throw new NoSuchElementException();

                lastRow = next;
                next = null;
                return lastRow;
            }
        }
    }

    protected void restoreState(int remaining, boolean lastWasRecorded)
    {
        this.remaining = remaining;
        this.lastWasRecorded = lastWasRecorded;
    }

    public boolean isExhausted()
    {
        return exhausted || remaining == 0;
    }

    public int maxRemaining()
    {
        return remaining;
    }

    private int nextPageSize(int pageSize)
    {
        return Math.min(remaining, pageSize) + (lastWasRecorded ? 1 : 0);
    }

    protected ColumnFamilyStore getStore()
    {
        return Keyspace.openAndGetStore(cfm);
    }

    protected abstract DataIterator queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery) throws RequestValidationException, RequestExecutionException;

    protected abstract boolean shouldSkip(DecoratedKey key, Row row);
    protected abstract boolean recordLast(DecoratedKey key, Row row);
}
