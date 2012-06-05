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
package org.apache.cassandra.db.filter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SSTableSliceIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceQueryFilter implements IFilter
{
    private static final Logger logger = LoggerFactory.getLogger(SliceQueryFilter.class);
    public static final Serializer serializer = new Serializer();

    public volatile ByteBuffer start;
    public volatile ByteBuffer finish;
    public final boolean reversed;
    public volatile int count;
    private final int compositesToGroup;
    // This is a hack to allow rolling upgrade with pre-1.2 nodes
    private final int countMutliplierForCompatibility;

    // Not serialized, just a ack for range slices to find the number of live column counted, even when we group
    private ColumnCounter columnCounter;

    public SliceQueryFilter(ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(start, finish, reversed, count, -1, 1);
    }

    public SliceQueryFilter(ByteBuffer start, ByteBuffer finish, boolean reversed, int count, int compositesToGroup, int countMutliplierForCompatibility)
    {
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
        this.compositesToGroup = compositesToGroup;
        this.countMutliplierForCompatibility = countMutliplierForCompatibility;
    }

    public SliceQueryFilter withUpdatedCount(int newCount)
    {
        return new SliceQueryFilter(start, finish, reversed, newCount, compositesToGroup, countMutliplierForCompatibility);
    }

    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key)
    {
        return Memtable.getSliceIterator(key, cf, this);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableSliceIterator(sstable, key, start, finish, reversed);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableSliceIterator(sstable, file, key, start, finish, reversed, indexEntry);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // we clone shallow, then add, under the theory that generally we're interested in a relatively small number of subcolumns.
        // this may be a poor assumption.
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (reversed)
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = Lists.reverse(columnsAsList).iterator();
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // iterate until we get to the "real" start column
        Comparator<ByteBuffer> comparator = reversed ? superColumn.getComparator().reverseComparator : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), start) >= 0)
            {
                subcolumns = Iterators.concat(Iterators.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above.  either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    public Comparator<IColumn> getColumnComparator(AbstractType<?> comparator)
    {
        return reversed ? comparator.columnReverseComparator : comparator.columnComparator;
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        AbstractType<?> comparator = container.getComparator();

        if (compositesToGroup < 0)
            columnCounter = new ColumnCounter();
        else if (compositesToGroup == 0)
            columnCounter = new ColumnCounter.GroupByPrefix(null, 0);
        else
            columnCounter = new ColumnCounter.GroupByPrefix((CompositeType)comparator, compositesToGroup);

        while (reducedColumns.hasNext())
        {
            if (columnCounter.count() >= count)
                break;

            IColumn column = reducedColumns.next();
            if (logger.isDebugEnabled())
                logger.debug(String.format("collecting %s of %s: %s",
                                           columnCounter.count(), count, column.getString(comparator)));

            if (finish.remaining() > 0
                && ((!reversed && comparator.compare(column.name(), finish) > 0))
                    || (reversed && comparator.compare(column.name(), finish) < 0))
                break;

            columnCounter.countColum(column, container);

            // but we need to add all non-gc-able columns to the result for read repair:
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }

    public int lastCounted()
    {
        return columnCounter == null ? 0 : columnCounter.count();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
               "start=" + start +
               ", finish=" + finish +
               ", reversed=" + reversed +
               ", count=" + count + "]";
    }

    public boolean isReversed()
    {
        return reversed;
    }

    public void updateColumnsLimit(int newLimit)
    {
        count = newLimit;
    }

    public static class Serializer implements IVersionedSerializer<SliceQueryFilter>
    {
        public void serialize(SliceQueryFilter f, DataOutput dos, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(f.start, dos);
            ByteBufferUtil.writeWithShortLength(f.finish, dos);
            dos.writeBoolean(f.reversed);
            int count = f.count;
            if (f.compositesToGroup > 0 && version < MessagingService.VERSION_12)
                count *= f.countMutliplierForCompatibility;
            dos.writeInt(count);

            if (version < MessagingService.VERSION_12)
                return;

            dos.writeInt(f.compositesToGroup);
        }

        public SliceQueryFilter deserialize(DataInput dis, int version) throws IOException
        {
            ByteBuffer start = ByteBufferUtil.readWithShortLength(dis);
            ByteBuffer finish = ByteBufferUtil.readWithShortLength(dis);
            boolean reversed = dis.readBoolean();
            int count = dis.readInt();
            int compositesToGroup = -1;
            if (version >= MessagingService.VERSION_12)
                compositesToGroup = dis.readInt();

            return new SliceQueryFilter(start, finish, reversed, count, compositesToGroup, 1);
        }

        public long serializedSize(SliceQueryFilter f, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;

            int size = 0;
            int startSize = f.start.remaining();
            int finishSize = f.finish.remaining();

            size += sizes.sizeof((short) startSize) + startSize;
            size += sizes.sizeof((short) finishSize) + finishSize;
            size += sizes.sizeof(f.reversed);
            size += sizes.sizeof(f.count);

            if (version >= MessagingService.VERSION_12)
                size += sizes.sizeof(f.compositesToGroup);
            return size;
        }
    }
}
