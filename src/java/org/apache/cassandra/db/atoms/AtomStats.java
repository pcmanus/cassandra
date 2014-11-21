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

/**
 * General statistics on atoms.
 */
public class AtomStats
{
    // We should use this sparingly obviously
    public static final AtomStats NO_STATS = new AtomStats(Cells.NO_TIMESTAMP,
                                                           Cells.NO_TIMESTAMP,
                                                           Cells.NO_DELETION_TIME,
                                                           Cells.NO_DELETION_TIME,
                                                           Cells.NO_TTL);

    public final long minTimestamp;
    public final long maxTimestamp;

    public final int minLocalDeletionTime;
    public final int maxLocalDeletionTime;

    public final int minTTL;

    private AtomStats(long minTimestamp,
                      long maxTimestam,
                      int minLocalDeletionTime,
                      int maxLocalDeletionTime,
                      int minTTL)
    {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.minTTL = minTTL;
    }

    public AtomStats mergeWith(AtomStats that)
    {
        long minTimestamp = this.minTimestamp == Cells.NO_TIMESTAMP
                          ? that.minTimestamp
                          : (that.minTimestamp == Cells.NO_TIMESTAMP ? this.minTimestamp : Math.min(this.minTimestamp, that.minTimestamp));
        long maxTimestamp = this.maxTimestamp == Cells.NO_TIMESTAMP
                          ? that.maxTimestamp
                          : (that.maxTimestamp == Cells.NO_TIMESTAMP ? this.maxTimestamp : Math.max(this.maxTimestamp, that.maxTimestamp));

        long minDelTime = this.minLocalDeletionTime == Cells.NO_DELETION_TIME
                         ? that.minLocalDeletionTime
                         : (that.minLocalDeletionTime == Cells.NO_DELETION_TIME ? this.minLocalDeletionTime : Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));
        long maxDelTime = this.maxLocalDeletionTime == Cells.NO_DELETION_TIME
                         ? that.maxLocalDeletionTime
                         : (that.maxLocalDeletionTime == Cells.NO_DELETION_TIME ? this.maxLocalDeletionTime : Math.max(this.maxLocalDeletionTime, that.maxLocalDeletionTime));

        int minTTL = this.minTTL == Cells.NO_TTL
                   ? that.minTTL
                   : (that.minTTL == Cells.NO_TTL ? this.minTTL : Math.min(this.minTTL, that.minTTL));

        return new AtomStats(minTimestamp,
                             maxTimestamp,
                             minDelTime,
                             maxDelTime,
                             minTTL);
    }

    public static class Collector
    {
        private boolean timestampIsSet;
        private long minTimestamp;
        private long maxTimestamp;

        private boolean delTimeIsSet;
        private int minDeletionTime;
        private int maxDeletionTime;

        private int minTTL;

        public void updateTimestamp(long timestamp)
        {
            if (timestamp == Cells.NO_TIMESTAMP)
                return;

            if (timestampIsSet)
            {
                this.minTimestamp = Math.min(minTimestamp, timestamp);
                this.minTimestamp = Math.min(maxTimestamp, timestamp);
            }
            else
            {
                this.minTimestamp = this.maxTimestamp = timestamp;
                timestampIsSet = true;
            }
        }

        public void updateLocalDeletionTime(int deletionTime)
        {
            if (deletionTime == Cells.NO_DELETION_TIME)
                return;

            if (delTimeIsSet)
            {
                this.minDelTime = Math.min(minDelTime, minDelTime);
                this.minDelTime = Math.min(maxDelTime, minDelTime);
            }
            else
            {
                this.minDelTime = this.maxDelTime = deletionTime;
                delTimeIsSet = true;
            }
        }

        public void updateDeletionTime(DeletionTime deletionTime)
        {
            if (deletionTime.isLive())
                return;

            updateTimestamp(deletionTime.markedForDeleteAt());
            updateLocalDeletionTime(deletionTime.localDeletionTime());

            if (deletionTime == Cells.NO_DELETION_TIME)
                return;

            if (delTimeIsSet)
            {
                this.minDelTime = Math.min(minDelTime, minDelTime);
                this.minDelTime = Math.min(maxDelTime, minDelTime);
            }
            else
            {
                this.minDelTime = this.maxDelTime = deletionTime;
                delTimeIsSet = true;
            }
        }

        public void updateTTL(int ttl)
        {
            if (ttl <= Cells.NO_TTL)
                return;

            minTTL = Math.min(minTTL, ttl);
        }

        public AtomStats get()
        {
            return new AtomStats(timestampIsSet ? minTimestamp : Cells.NO_TIMESTAMP,
                                 timestampIsSet ? maxTimestamp : Cells.NO_TIMESTAMP,
                                 delTimeIsSet ? minDelTime : Cells.NO_DELETION_TIME,
                                 delTimeIsSet ? maxDelTime : Cells.NO_DELETION_TIME,
                                 minTTL);
        }
    }
}
