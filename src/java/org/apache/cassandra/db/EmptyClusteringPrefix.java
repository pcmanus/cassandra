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

import org.apache.cassandra.config.CFMetaData;

public class EmptyClusteringPrefix extends AbstractClusteringPrefix
{
    public static final EmptyClusteringPrefix STATIC_PREFIX = new EmptyClusteringPrefix(EOC.NONE)
    {
        @Override
        public String toString(CFMetaData metadata)
        {
            return "STATIC";
        }
    };

    public static final EmptyClusteringPrefix EMPTY = new EmptyClusteringPrefix(EOC.NONE);
    public static final EmptyClusteringPrefix BOTTOM = new EmptyClusteringPrefix(EOC.START);
    public static final EmptyClusteringPrefix TOP = new EmptyClusteringPrefix(EOC.END);

    private final EOC eoc;

    private EmptyClusteringPrefix(EOC eoc)
    {
        this.eoc = eoc;
    }

    public static EmptyClusteringPrefix forEOC(EOC eoc)
    {
        switch (eoc)
        {
            case START: return BOTTOM;
            case NONE:  return EMPTY;
            case END:   return TOP;
        }
        throw new AssertionError();
    }

    public int size()
    {
        return 0;
    }

    public ByteBuffer get(int i)
    {
        return null;
    }

    @Override
    public ClusteringPrefix.EOC eoc()
    {
        return eoc;
    }

    public ClusteringPrefix takeAlias()
    {
        return this;
    }

    @Override
    public ClusteringPrefix withEOC(EOC eoc)
    {
        if (this.eoc == eoc)
            return this;

        switch (eoc)
        {
            case START: return BOTTOM;
            case NONE:  return EMPTY;
            case END:   return TOP;
        }
        throw new AssertionError();
    }

    public long unsharedHeapSize()
    {
        return 0;
    }

    @Override
    public String toString(CFMetaData metadata)
    {
        switch (eoc)
        {
            case START: return "BOTTOM";
            case NONE:  return "EMPTY";
            case END:   return "TOP";
        }
        throw new AssertionError();
    }
}
