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
package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tombstone garbage collection parameters for a particular compaction task.
 * <p>
 * This mainly fixes the value for "now" and the "gcGrace" so it stays fixed during
 * a given compaction. This can also be use to force no garbage collection at all.
 */
public class GCParams
{
    public static final GCParams NO_GC = new GCParams(Integer.MIN_VALUE, 0);

    @VisibleForTesting
    public static final GCParams GC_ALL = new GCParams(Integer.MAX_VALUE, 0);

    private final int nowInSec;
    private final int gcGrace;

    private GCParams(int nowInSec, int gcGrace)
    {
        this.nowInSec = nowInSec;
        this.gcGrace = gcGrace;
    }

    public static GCParams defaultFor(ColumnFamilyStore cfs)
    {
        return defaultFor(cfs, FBUtilities.nowInSeconds());
    }

    public static GCParams defaultFor(ColumnFamilyStore cfs, int nowInSec)
    {
        // 2ndary indexes are local so we have no need for a gcGrace
        int gcGrace = cfs.isIndex() ? 0 : cfs.metadata.params.gcGraceSeconds;
        return new GCParams(nowInSec, gcGrace);
    }

    /**
     * The time of the compaction (in seconds).
     */
    public int nowInSeconds()
    {
        return nowInSec;
    }

    /**
     * The time we should wait from a data purging reference time before considering it for purging.
     */
    public int gcGraceSeconds()
    {
        return gcGrace;
    }

    /**
     * Returns whether a sstable is fully expired base on that sstable statistics.
     *
     * @param sstable the sstable to check.
     * @return {@code true} if the everything in the sstable can be purged given the
     * garbage collection parameters, {@code false} otherwise.
     */
    public boolean isFullyExpired(SSTableReader sstable)
    {
        // Everything is expired if the maximum time at which something can be purged is before now
        // (a sstable that has anything live will have a maxPurgingTime of MAX_VALUE in particular).
        return sstable.getSSTableMetadata().maxPurgingTime < nowInSec;
    }
}
