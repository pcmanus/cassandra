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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.google.common.base.Function;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public class TreeMapBackedSortedColumns extends AbstractThreadUnsafeSortedColumns implements ISortedColumns
{
    private final TreeMap<ByteBuffer, Column> map;

    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new TreeMapBackedSortedColumns(comparator);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, Column> sortedMap, boolean insertReversed)
        {
            return new TreeMapBackedSortedColumns(sortedMap);
        }
    };

    public static ISortedColumns.Factory factory()
    {
        return factory;
    }

    public AbstractType<?> getComparator()
    {
        return (AbstractType<?>)map.comparator();
    }

    private TreeMapBackedSortedColumns(AbstractType<?> comparator)
    {
        this.map = new TreeMap<ByteBuffer, Column>(comparator);
    }

    private TreeMapBackedSortedColumns(SortedMap<ByteBuffer, Column> columns)
    {
        this.map = new TreeMap<ByteBuffer, Column>(columns);
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory();
    }

    public ISortedColumns cloneMe()
    {
        return new TreeMapBackedSortedColumns(map);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    public void addColumn(Column column, Allocator allocator)
    {
        addColumn(column, allocator, SecondaryIndexManager.nullUpdater);
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column
    */
    public long addColumn(Column column, Allocator allocator, SecondaryIndexManager.Updater indexer)
    {
        ByteBuffer name = column.name();
        // this is a slightly unusual way to structure this; a more natural way is shown in ThreadSafeSortedColumns,
        // but TreeMap lacks putAbsent.  Rather than split it into a "get, then put" check, we do it as follows,
        // which saves the extra "get" in the no-conflict case [for both normal and super columns],
        // in exchange for a re-put in the SuperColumn case.
        Column oldColumn = map.put(name, column);
        if (oldColumn == null)
            return column.dataSize();

        // calculate reconciled col from old (existing) col and new col
        Column reconciledColumn = column.reconcile(oldColumn, allocator);
        map.put(name, reconciledColumn);
        // for memtable updates we only care about oldcolumn, reconciledcolumn, but when compacting
        // we need to make sure we update indexes no matter the order we merge
        if (reconciledColumn == column)
            indexer.update(oldColumn, reconciledColumn);
        else
            indexer.update(column, reconciledColumn);
        return reconciledColumn.dataSize() - oldColumn.dataSize();
    }

    public long addAllWithSizeDelta(ISortedColumns cm, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
    {
        delete(cm.getDeletionInfo());
        for (Column column : cm.getSortedColumns())
            addColumn(transformation.apply(column), allocator, indexer);

        // we don't use this for memtables, so we don't bother computing size
        return Long.MIN_VALUE;
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(ISortedColumns cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        // We are not supposed to put the newColumn is either there was not
        // column or the column was not equal to oldColumn (to be coherent
        // with other implementation). We optimize for the common case where
        // oldColumn do is present though.
        Column previous = map.put(oldColumn.name(), newColumn);
        if (previous == null)
        {
            map.remove(oldColumn.name());
            return false;
        }
        if (!previous.equals(oldColumn))
        {
            map.put(oldColumn.name(), previous);
            return false;
        }
        return true;
    }

    public Column getColumn(ByteBuffer name)
    {
        return map.get(name);
    }

    public void removeColumn(ByteBuffer name)
    {
        map.remove(name);
    }

    public void clear()
    {
        map.clear();
    }

    public int size()
    {
        return map.size();
    }

    public Collection<Column> getSortedColumns()
    {
        return map.values();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return map.descendingMap().values();
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return map.navigableKeySet();
    }

    public Iterator<Column> iterator()
    {
        return map.values().iterator();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map, slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map.descendingMap(), slices);
    }
}
