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
        assert deletionTime == Cells.NO_DELETION_TIME || !value.hasRemaining() || ttl != Cells.NO_TTL : String.format("v=%s, dt=%d, ttl=%d", value, deletionTime, ttl);
        values[idx] = value;
        timestamps[idx] = timestamp;
        delTimesAndTTLs[2 * idx] = deletionTime;
        delTimesAndTTLs[(2 * idx) + 1] = ttl;
    }

    public boolean hasCell(int idx)
    {
        return idx < values.length && values[idx] != null;
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
        return delTimesAndTTLs[2 * idx];
    }

    public int ttl(int idx)
    {
        return delTimesAndTTLs[(2 * idx) + 1];
    }

    private void setDefaults(int from, int to)
    {
        Arrays.fill(values, from, to, null);
        Arrays.fill(timestamps, from, to, Cells.NO_TIMESTAMP);

        for (int i = from; i < to; i++)
        {
            delTimesAndTTLs[2 * i] = Cells.NO_DELETION_TIME;
            delTimesAndTTLs[(2 * i) + 1] = Cells.NO_TTL;
        }
    }

    private void ensureCapacity(int idxToSet)
    {
        int originalCapacity = values.length;
        if (idxToSet < originalCapacity)
            return;

        int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, idxToSet);

        values = Arrays.copyOf(values, newCapacity);
        timestamps = Arrays.copyOf(timestamps, newCapacity);
        delTimesAndTTLs = Arrays.copyOf(delTimesAndTTLs, newCapacity * 2);

        setDefaults(originalCapacity, newCapacity);
    }

    // Swap cell i and j
    public void swapCell(int i, int j)
    {
        ensureCapacity(Math.max(i, j));

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
        mergeCell(this, i, this, j, this, j, nowInSec);
    }

    public static void mergeCell(CellData d1, int i1, CellData d2, int i2, CellData merged, int iMerged, int nowInSec)
    {
        if (!d1.hasCell(i1))
        {
            if (d2.hasCell(i2))
                d2.moveCell(i2, merged, iMerged);
            return;
        }
        if (!d2.hasCell(i2))
        {
            d1.moveCell(i1, merged, iMerged);
            return;
        }

        long ts1 = d1.timestamps[i1], ts2 = d2.timestamps[i2];
        if (ts1 != ts1)
        {
            if (ts1 < ts2)
                d2.moveCell(i2, merged, iMerged);
            else
                d1.moveCell(i1, merged, iMerged);
            return;
        }
        boolean live1 = d1.delTimesAndTTLs[i1 * 2] > nowInSec;
        boolean live2 = d2.delTimesAndTTLs[i2 * 2] > nowInSec;
        if (live1 != live2)
        {
            if (live1)
                d1.moveCell(i1, merged, iMerged);
            else
                d2.moveCell(i2, merged, iMerged);
            return;
        }

        if (d1.values[i1].compareTo(d2.values[i2]) < 0)
            d2.moveCell(i2, merged, iMerged);
        else
            d1.moveCell(i1, merged, iMerged);
    }

    // Move cell i into j
    public void moveCell(int i, int j)
    {
        moveCell(i, this, j);
    }

    public void moveCell(int i, CellData target, int j)
    {
        if (!hasCell(i) || (target == this && i == j))
            return;

        target.ensureCapacity(j);

        target.values[j] = values[i];
        target.timestamps[j] = timestamps[i];
        target.delTimesAndTTLs[2 * j] = delTimesAndTTLs[2 * i];
        target.delTimesAndTTLs[(2 * j) + 1] = delTimesAndTTLs[(2 * i) + 1];
    }

    public int dataSize()
    {
        int size = 16 * values.length; // timestamp, ttl and deletion time
        for (int i = 0; i < values.length; i++)
            if (values[i] != null)
                size += values[i].remaining();
        return size;
    }

    public void clear()
    {
        setDefaults(0, values.length);
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
