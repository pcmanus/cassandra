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

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.ColumnStats;

/**
 * General statistics on atoms.
 */
public class AtomStats
{
    // We should use this sparingly obviously
    public static final AtomStats NO_STATS = new AtomStats(Cells.NO_TIMESTAMP,
                                                           Cells.NO_DELETION_TIME,
                                                           Cells.NO_TTL);

    public final long minTimestamp;
    public final int minLocalDeletionTime;
    public final int minTTL;

    public AtomStats(long minTimestamp,
                      int minLocalDeletionTime,
                      int minTTL)
    {
        this.minTimestamp = minTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.minTTL = minTTL;
    }

    public AtomStats mergeWith(AtomStats that)
    {
        long minTimestamp = this.minTimestamp == Cells.NO_TIMESTAMP
                          ? that.minTimestamp
                          : (that.minTimestamp == Cells.NO_TIMESTAMP ? this.minTimestamp : Math.min(this.minTimestamp, that.minTimestamp));

        int minDelTime = this.minLocalDeletionTime == Cells.NO_DELETION_TIME
                       ? that.minLocalDeletionTime
                       : (that.minLocalDeletionTime == Cells.NO_DELETION_TIME ? this.minLocalDeletionTime : Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));

        int minTTL = this.minTTL == Cells.NO_TTL
                   ? that.minTTL
                   : (that.minTTL == Cells.NO_TTL ? this.minTTL : Math.min(this.minTTL, that.minTTL));

        return new AtomStats(minTimestamp,
                             minDelTime,
                             minTTL);
    }

    public static class Collector
    {
        private ColumnStats.MinTracker<Long> minTimestamp = new ColumnStats.MinTracker<>(Cells.NO_TIMESTAMP);
        private ColumnStats.MinTracker<Integer> minDeletionTime = new ColumnStats.MinTracker<>(Cells.NO_DELETION_TIME);
        private ColumnStats.MinTracker<Integer> minTTL = new ColumnStats.MinTracker<>(0);

        public void updateTimestamp(long timestamp)
        {
            if (timestamp == Cells.NO_TIMESTAMP)
                return;

            minTimestamp.update(timestamp);
        }

        public void updateLocalDeletionTime(int deletionTime)
        {
            if (deletionTime == Cells.NO_DELETION_TIME)
                return;

            minDeletionTime.update(deletionTime);
        }

        public void updateDeletionTime(DeletionTime deletionTime)
        {
            if (deletionTime.isLive())
                return;

            updateTimestamp(deletionTime.markedForDeleteAt());
            updateLocalDeletionTime(deletionTime.localDeletionTime());
        }

        public void updateTTL(int ttl)
        {
            if (ttl <= Cells.NO_TTL)
                return;

            minTTL.update(ttl);
        }

        public AtomStats get()
        {
            return new AtomStats(minTimestamp.get(),
                                 minDeletionTime.get(),
                                 minTTL.get());
        }
    }
}
