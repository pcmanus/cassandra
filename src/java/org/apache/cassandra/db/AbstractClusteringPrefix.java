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
import java.util.Objects;
import java.security.MessageDigest;

import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractClusteringPrefix implements ClusteringPrefix
{
    public ClusteringPrefix clustering()
    {
        return this;
    }

    public EOC eoc()
    {
        return EOC.NONE;
    }

    public ClusteringPrefix withEOC(EOC eoc)
    {
        if (this.eoc() == eoc)
            return this;

        ByteBuffer[] values = new ByteBuffer[size()];
        for (int i = 0; i < size(); i++)
            values[i] = get(i);

        return new SimpleClusteringPrefix(values, size(), eoc);
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public ClusteringPrefix takeAlias()
    {
        ByteBuffer[] values = new ByteBuffer[size()];
        for (int i = 0; i < size(); i++)
            values[i] = get(i);
        return new SimpleClusteringPrefix(values, size(), eoc());
    }

    public void digest(MessageDigest digest)
    {
        for (int i = 0; i < size(); i++)
            digest.update(get(i).duplicate());
        FBUtilities.updateWithByte(digest, eoc().ordinal());
    }

    public long unsharedHeapSize()
    {
        // unsharedHeapSize is used inside the cache and in memtables. The implementations of ClusteringPrefix that are
        // safe to use there (SimpleClusteringPrefix and MemtableRow.BufferClusteringPrefix) overwrite this.
        throw new UnsupportedOperationException();
    }

    @Override
    public final int hashCode()
    {
        int result = 31;
        for (int i = 0; i < size(); i++)
            result += 31 * Objects.hashCode(get(i));
        return 31 * result + Objects.hashCode(eoc());
    }

    @Override
    public final boolean equals(Object o)
    {

        if(!(o instanceof ClusteringPrefix))
            return false;

        ClusteringPrefix that = (ClusteringPrefix)o;
        if (this.size() != that.size())
            return false;

        for (int i = 0; i < size(); i++)
            if (!Objects.equals(this.get(i), that.get(i)))
                return false;

        return this.eoc() == that.eoc();
    }
}

