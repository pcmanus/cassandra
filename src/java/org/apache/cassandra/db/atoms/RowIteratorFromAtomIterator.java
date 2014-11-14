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
package org.apache.cassandra.db.atoms;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

class RowIteratorFromAtomIterator extends AbstractIterator<Row> implements RowIterator
{
    private final AtomIterator iter;

    private final HideDeletedRow row;

    public RowIteratorFromAtomIterator(AtomIterator iter, int nowInSec)
    {
        this.iter = iter;
        this.row = new HideDeletedRow(nowInSec);
    }

    public CFMetaData metadata()
    {
        return iter.metadata();
    }

    public boolean isReverseOrder()
    {
        return iter.isReverseOrder();
    }

    public DecoratedKey partitionKey()
    {
        return iter.partitionKey();
    }

    public Row staticRow()
    {
        return row.setTo(iter.staticRow());
    }

    protected Row computeNext()
    {
        while (iter.hasNext())
        {
            Atom next = iter.next();
            if (next.kind() != Atom.Kind.ROW)
                continue;

            Row toReturn = row.setTo((Row)next);
            if (!toReturn.isEmpty())
                return toReturn;
        }
        return endOfData();
    }

    public void close() throws IOException
    {
        iter.close();
    }

    private static class HideDeletedRow implements Row
    {
        private Row row;
        private final int nowInSec;

        private final HideDeletedColumnData data = new HideDeletedColumnData();

        public HideDeletedRow(int nowInSec)
        {
            this.nowInSec = nowInSec;
        }

        public HideDeletedRow setTo(Row row)
        {
            this.row = row;
            return this;
        }

        public Atom.Kind kind()
        {
            return Atom.Kind.ROW;
        }

        public ClusteringPrefix clustering()
        {
            return row.clustering();
        }

        public long timestamp()
        {
            return Long.MIN_VALUE;
        }

        public boolean isEmpty()
        {
            return !iterator().hasNext();
        }

        public ColumnData data(ColumnDefinition c)
        {
            data.setTo(row.data(c));
            return data.hasData() ? data : null;
        }

        public Iterator<ColumnData> iterator()
        {
            return new AbstractIterator<ColumnData>()
            {
                private final Iterator<ColumnData> iter = row.iterator();

                protected ColumnData computeNext()
                {
                    while (iter.hasNext())
                    {
                        data.setTo(iter.next());
                        if (data.hasData())
                            return data;
                    }
                    return endOfData();
                }
            };
        }

        @Override
        public Row takeAlias()
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        private class HideDeletedColumnData implements ColumnData
        {
            private ColumnData data;

            public ColumnData setTo(ColumnData data)
            {
                this.data = data;
                return this;
            }

            public ColumnDefinition column()
            {
                return data.column();
            }

            public boolean hasData()
            {
                for (int i = 0; i < data.size(); i++)
                    if (Cells.isLive(data.cell(i), nowInSec))
                        return true;

                return false;
            }

            public int size()
            {
                int size = 0;
                for (int i = 0; i < data.size(); i++)
                    if (Cells.isLive(data.cell(i), nowInSec))
                        size++;
                return size;
            }

            public Cell cell(int i)
            {
                int count = 0;
                for (int j = 0; j < data.size(); j++)
                {
                    Cell cell = data.cell(j);
                    if (Cells.isLive(cell, nowInSec))
                    {
                        if (count == i)
                            return cell;
                        count++;
                    }
                }
                throw new AssertionError();
            }

            public DeletionTime complexDeletionTime()
            {
                return DeletionTime.LIVE;
            }
        }
    }
}
