/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachedRow;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SimpleAbstractColumnIterator;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.util.FileUtils;

public class RowCacheCollationController extends AbstractCollationController
{
    private static Logger logger = LoggerFactory.getLogger(RowCacheCollationController.class);

    private final CachedRow cachedRow;
    private final ColumnFamilyStore.ViewFragment view;

    public RowCacheCollationController(ColumnFamilyStore cfs, ColumnFamilyStore.ViewFragment view, CachedRow cachedRow, QueryFilter filter, int gcBefore)
    {
        super(cfs, false, filter, gcBefore);
        this.cachedRow = cachedRow;
        this.view = view;
    }

    protected ColumnFamilyStore.ViewFragment getView()
    {
        return view;
    }

    protected void addTimeOrderedData(ColumnFamilyStore.ViewFragment view, ColumnFamily container, List<OnDiskAtomIterator> iterators, QueryFilter reducedFilter)
    {
        // read cached data
        long currentMaxTs = cachedRow.getMaxTimestamp();
        reduceNameFilter(reducedFilter, container, currentMaxTs);

        if (!((NamesQueryFilter) reducedFilter.filter).columns.isEmpty())
        {
            OnDiskAtomIterator iter = reducedFilter.getRowCacheColumnIterator(cfs.metadata, cachedRow, iterators.isEmpty());
            if (iter.getColumnFamily() != null)
            {
                iterators.add(iter);
                container.delete(iter.getColumnFamily());
                while (iter.hasNext())
                    container.addColumn((IColumn) iter.next());
            }
        }
    }

    protected void postTimeOrderedQuery(ColumnFamily returnCF)
    {
    }

    protected void addData(ColumnFamilyStore.ViewFragment view, ColumnFamily returnCF, List<OnDiskAtomIterator> iterators)
    {
        /* add the cached merged row*/
        OnDiskAtomIterator iter = filter.getRowCacheColumnIterator(cfs.metadata, cachedRow, iterators.isEmpty());
        if (iter.getColumnFamily() != null)
        {
            iterators.add(iter);
            returnCF.delete(iter.getColumnFamily());
        }
    }
}
