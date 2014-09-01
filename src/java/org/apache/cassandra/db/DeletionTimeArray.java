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

import java.util.Arrays;

/**
 * Utility class to store an array of deletion times a bit efficiently.
 */
public class DeletionTimeArray
{
    private long[] markedForDeleteAts;
    private int[] delTimes;

    public DeletionTimeArray(int initialCapacity)
    {
        this.markedForDeleteAts = new long[initialCapacity];
        this.delTimes = new int[initialCapacity];
        clear();
    }

    public void clear(int i)
    {
        markedForDeleteAts[i] = Long.MIN_VALUE;
        delTimes[i] = Integer.MAX_VALUE;
    }

    public void set(int i, DeletionTime dt)
    {
        this.markedForDeleteAts[i] = dt.markedForDeleteAt();
        this.delTimes[i] = dt.localDeletionTime();
    }

    public int size()
    {
        return markedForDeleteAts.length;
    }

    public void resize(int newSize)
    {
        int prevSize = size();

        markedForDeleteAts = Arrays.copyOf(markedForDeleteAts, newSize);
        delTimes = Arrays.copyOf(delTimes, newSize);

        Arrays.fill(markedForDeleteAts, prevSize, newSize, Long.MIN_VALUE);
        Arrays.fill(delTimes, prevSize, newSize, Integer.MAX_VALUE);
    }

    public Cursor newCursor()
    {
        return new Cursor();
    }

    public boolean supersedes(int i, DeletionTime dt)
    {
        return markedForDeleteAts[i] > dt.markedForDeleteAt();
    }

    public boolean supersedes(int i, int j)
    {
        return markedForDeleteAts[i] > markedForDeleteAts[j];
    }

    public boolean isLive(int i)
    {
        return markedForDeleteAts[i] > Long.MIN_VALUE;
    }

    public void clear()
    {
        Arrays.fill(markedForDeleteAts, Long.MIN_VALUE);
        Arrays.fill(delTimes, Integer.MAX_VALUE);
    }

    public void copy(DeletionTimeArray other)
    {
        assert size() == other.size();
        for (int i = 0; i < size(); i++)
        {
            markedForDeleteAts[i] = other.markedForDeleteAts[i];
            delTimes[i] = other.delTimes[i];
        }
    }

    public class Cursor extends DeletionTime
    {
        public int i;

        public Cursor setTo(int i)
        {
            this.i = i;
            return this;
        }

        public long markedForDeleteAt()
        {
            return markedForDeleteAts[i];
        }

        public int localDeletionTime()
        {
            return delTimes[i];
        }

        public DeletionTime takeAlias()
        {
            return new SimpleDeletionTime(markedForDeleteAt(), localDeletionTime());
        }
    }
}
