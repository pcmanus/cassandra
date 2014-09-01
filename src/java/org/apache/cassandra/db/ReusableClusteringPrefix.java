
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
import java.util.Arrays;

import org.apache.cassandra.utils.ObjectSizes;

public class ReusableClusteringPrefix implements ClusteringPrefix
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ReusableClusteringPrefix(0));

    private final ByteBuffer[] values;
    private int size;
    private EOC eoc;

    public ReusableClusteringPrefix(int maxSize)
    {
        this.values = new ByteBuffer[maxSize];
    }

    public ClusteringPrefix clustering()
    {
        return this;
    }

    public int size()
    {
        return size;
    }

    public ByteBuffer get(int i)
    {
        return values[i];
    }

    public EOC eoc()
    {
        return eoc;
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public ClusteringPrefix withEOC(EOC eoc)
    {
        throw new UnsupportedOperationException();
    }

    public long unsharedHeapSize()
    {
        // As this is a reusable structure, only the object itself is "unshared"
        return EMPTY_SIZE;
    }

    public void copy(ClusteringPrefix prefix)
    {
        for (int i = 0; i < prefix.size(); i++)
            values[i] = prefix.get(i);

        size = prefix.size();
        eoc = prefix.eoc();
    }

    public ClusteringPrefix takeAlias()
    {
        return new SimpleClusteringPrefix(Arrays.copyOf(values, size), size, eoc);
    }
}

