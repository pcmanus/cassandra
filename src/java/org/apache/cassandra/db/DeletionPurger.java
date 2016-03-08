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

/**
 * A {@code DeletionPurger} decides whether a given piece of deleted data
 * (tombstone (all type of them) or expiring cell) might be purged.
 */
public interface DeletionPurger
{
    public static final DeletionPurger PURGE_ALL = (ts, prt, ttl) -> true;

    /**
     * Whether the deleted data (can be a cell tombstone, an expired cell, a range tombstone, a row liveness info or
     * a collection deletion) having the provided parameters can be purged.
     *
     * @param timestamp the data timestamp.
     * @param purgingReferenceTime the data purging reference time (that is, the time from which at least gcGrace should
     * have elapsed for purging to be allowed, i.e. the time at which the deleted data has been initially created).
     * @param ttl the ttl if the data is expiring ({@code Cell.NO_TTL} otherwise).
     *
     * @return whether the deleted data having those parameters can be purged.
     */
    public boolean shouldPurge(long timestamp, int purgingReferenceTime, int ttl);

    public default boolean shouldPurge(DeletionTime dt)
    {
        return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime(), 0);
    }

    public default boolean shouldPurge(LivenessInfo liveness, int nowInSec)
    {
        return !liveness.isLive(nowInSec) && shouldPurge(liveness.timestamp(), liveness.purgingReferenceTime(), liveness.ttl());
    }
}
