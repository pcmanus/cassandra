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
import org.apache.cassandra.utils.ObjectSizes;

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

        int newCapacity = capacity == 0 ? 4 : (capacity * 3) / 2 + 1;

        values = Arrays.copyOf(values, newCapacity);
        timestamps = Arrays.copyOf(timestamps, newCapacity);
        delTimesAndTTLs = Arrays.copyOf(delTimesAndTTLs, newCapacity * 2);

        setDefaults(capacity, newCapacity);
    }

    // Swap cell i and j
    public void swapCell(int i, int j)
    {
        ByteBuffer value = values[j];
        long tstamp = timestamps[j];
        int delTime = delTimesAndTTLs[2 * j];
        int ttl = delTimesAndTTLs[(2 * j) + 1];

        moveCell(i, j);

        values[i] = value;
        timestamps[i] = tstamp;
        delTimesAndTTLs[2 * i] = delTime;
        delTimesAndTTLs[(2 * i) + 1] = ttl;
    }

    // Merge cell i into j
    public void mergeCell(int i, int j, int nowInSec)
    {
        long tsi = timestamps[i], tsj = timestamps[j];
        if (tsi != tsj)
        {
            if (tsi < tsj)
                moveCell(j, i);
            return;
        }
        boolean iLive = delTimesAndTTLs[i * 2] > nowInSec;
        boolean jLive = delTimesAndTTLs[j * 2] > nowInSec;
        if (iLive != jLive)
        {
            if (jLive)
                moveCell(j, i);
            return;
        }

        if (values[i].compareTo(values[j]) >= 0)
            moveCell(j, i);
    }

    // Move cell i into j
    public void moveCell(int i, int j)
    {
        values[j] = values[i];
        timestamps[j] = timestamps[i];
        delTimesAndTTLs[2 * j] = delTimesAndTTLs[2 * i];
        delTimesAndTTLs[(2 * j) + 1] = delTimesAndTTLs[(2 * i) + 1];
    }

    public int dataSize()
    {
        int size = 16 * values.length; // timestamp, ttl and deletion time
        for (int i = 0; i < values.length; i++)
            if (values[i] != null)
                size += values[i].remaining();
        return size;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return ObjectSizes.sizeOnHeapExcludingData(values)
             + ObjectSizes.sizeOfArray(timestamps)
             + ObjectSizes.sizeOfArray(delTimesAndTTLs);
    }

    static class ReusableCell implements Cell
    {
        CellData data;

        private ColumnDefinition column;
        protected int idx;

        ReusableCell setTo(CellData data, ColumnDefinition column, int idx)
        {
            if (!data.hasCell(idx))
                return null;

            this.data = data;
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
