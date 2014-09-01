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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A builder of ClusteringPrefix.
 */
public class CBuilder
{
    private final ClusteringComparator type;
    private final ByteBuffer[] values;
    private int size;
    private boolean built;

    public CBuilder(ClusteringComparator type)
    {
        this.type = type;
        this.values = new ByteBuffer[type.size()];
    }

    public int count()
    {
        return size;
    }

    public int remainingCount()
    {
        return values.length - size;
    }

    public ClusteringComparator comparator()
    {
        return type;
    }

    public CBuilder add(ByteBuffer value)
    {
        if (isDone())
            throw new IllegalStateException();
        values[size++] = value;
        return this;
    }

    public CBuilder add(Object value)
    {
        return add(((AbstractType)type.subtype(size)).decompose(value));
    }

    private boolean isDone()
    {
        return remainingCount() == 0 || built;
    }

    public ClusteringPrefix build()
    {
        return build(ClusteringPrefix.EOC.NONE);
    }

    public ClusteringPrefix build(ClusteringPrefix.EOC eoc)
    {
        // We don't allow to add more element to a builder that has been built so
        // that we don't have to copy values.
        built = true;

        return size == 0
             ? EmptyClusteringPrefix.forEOC(eoc)
             : new SimpleClusteringPrefix(values, size, eoc);
    }

    public ClusteringPrefix buildWith(ByteBuffer value)
    {
        return buildWith(value, ClusteringPrefix.EOC.NONE);
    }

    public ClusteringPrefix buildWith(ByteBuffer value, ClusteringPrefix.EOC eoc)
    {
        ByteBuffer[] newValues = Arrays.copyOf(values, size+1);
        newValues[size] = value;
        return new SimpleClusteringPrefix(newValues, size+1, eoc);
    }

    public ClusteringPrefix buildWith(List<ByteBuffer> newValues, ClusteringPrefix.EOC eoc)
    {
        ByteBuffer[] buffers = Arrays.copyOf(values, values.length);
        int newSize = size;
        for (ByteBuffer value : newValues)
            buffers[newSize++] = value;
        return new SimpleClusteringPrefix(buffers, newSize, eoc);
    }
}
