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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

/**
 * Contains (non-counter) cell data for one or more rows.
 */
class CellData
{
    private ByteBuffer[] values;
    private long[] timestamps;
    private int[] delTimesAndTTLs;

    CellData(int initialCellCapacity)
    {
        this.values = new ByteBuffer[initialCellCapacity];
        this.timestamps = new long[initialCellCapacity];
        this.delTimesAndTTLs = new int[2 * initialCellCapacity];

        setDefaults(0, initialCellCapacity);
    }

    public void setCell(int idx, ByteBuffer value, long timestamp, int deletionTime, int ttl)
    {
        ensureCapacity(idx);
        values[idx] = value;
        timestamps[idx] = timestamp;
        delTimesAndTTLs[idx] = deletionTime;
        delTimesAndTTLs[idx + 1] = ttl;
    }

    public boolean hasCell(int idx)
    {
        return values[idx] != null;
    }

    public ByteBuffer value(int idx)
    {
        return values[idx];
    }

    public long timestamp(int idx)
    {
        return timestamps[idx];
    }

    public int deletionTime(int idx)
    {
        return delTimesAndTTLs[idx];
    }

    public int ttl(int idx)
    {
        return delTimesAndTTLs[idx + 1];
    }

    private void setDefaults(int from, int to)
    {
        Arrays.fill(timestamps, from, to, Cells.NO_TIMESTAMP);

        for (int i = from; i < to; i++)
        {
            delTimesAndTTLs[i] = Cells.NO_DELETION_TIME;
            delTimesAndTTLs[i + 1] = Cells.NO_TTL;
        }
    }

    private void ensureCapacity(int idxToSet)
    {
        int capacity = values.length;
        if (idxToSet < capacity)
            return;

        int newCapacity = (capacity * 3) / 2 + 1;

        values = Arrays.copyOf(values, newCapacity);
        timestamps = Arrays.copyOf(timestamps, newCapacity);
        delTimesAndTTLs = Arrays.copyOf(delTimesAndTTLs, newCapacity * 2);

        setDefaults(capacity, newCapacity);
    }

    static class ReusableCell implements Cell
    {
        protected final CellData data;

        private ColumnDefinition column;
        protected int idx;

        ReusableCell(CellData data)
        {
            this.data = data;
        }

        ReusableCell setToPosition(ColumnDefinition column, int idx)
        {
            if (!data.hasCell(idx))
                return null;

            this.column = column;
            this.idx = idx;
            return this;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return false;
        }

        public ByteBuffer value()
        {
            return data.value(idx);
        }

        public long timestamp()
        {
            return data.timestamp(idx);
        }

        public int localDeletionTime()
        {
            return data.deletionTime(idx);
        }

        public int ttl()
        {
            return data.ttl(idx);
        }

        public CellPath path()
        {
            return null;
        }
    }
}

