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

import org.apache.cassandra.utils.ObjectSizes;

public class SimpleClusteringPrefix extends AbstractClusteringPrefix
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleClusteringPrefix(new ByteBuffer[0], EOC.NONE));

    private final ByteBuffer[] values;
    private final EOC eoc;

    public SimpleClusteringPrefix(ByteBuffer[] values, ClusteringPrefix.EOC eoc)
    {
        this.values = values;
        this.eoc = eoc;
    }

    public SimpleClusteringPrefix(ByteBuffer value)
    {
        this(new ByteBuffer[]{ value }, EOC.NONE);
    }

    public int size()
    {
        return values.size();
    }

    public ByteBuffer get(int i)
    {
        return values[i];
    }

    @Override
    public EOC eoc()
    {
        return eoc;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    @Override
    public ClusteringPrefix withEOC(EOC eoc)
    {
        if (this.eoc == eoc)
            return this;

        return new SimpleClusteringPrefix(values, eoc);
    }

    public ClusteringPrefix takeAlias()
    {
        return this;
    }
}
