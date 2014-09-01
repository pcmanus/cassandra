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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.db.atoms.Atom;
import org.apache.cassandra.io.ISerializer;

/**
 * A partition as stored in the row cache.
 */
public interface CachedPartition extends Partition, IRowCacheEntry
{
    // TODO
    public static final ISerializer<CachedPartition> cacheSerializer = null;

    public int rowCount();

    // The number of live rows in this cached partition. But please note that this always
    // count expiring cells as live, see CFS.isFilterFullyCoveredBy for the reason of this.
    public int rowsWithNonTombstoneCells();

    public Atom tail();

}
