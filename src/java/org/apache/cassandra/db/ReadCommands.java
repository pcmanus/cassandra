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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filters.DataLimits;
import org.apache.cassandra.db.filters.PartitionFilter;
import org.apache.cassandra.service.StorageService;

public abstract class ReadCommands
{
    private ReadCommands() {}

    public static SinglePartitionReadCommand fullPartitionRead(CFMetaData metadata, DecoratedKey key, int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static SinglePartitionReadCommand fullSlicesRead(CFMetaData metadata, DecoratedKey key, Slices slices, int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static ReadCommand allDataRead(CFMetaData metadata, int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // Only read the partition keys
    public static ReadCommand allKeysRead(CFMetaData metadata, int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static SinglePartitionReadCommand create(CFMetaData metadata, ByteBuffer key, PartitionFilter filter, DataLimits limits, int nowInSec)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return create(metadata, dk, filter, limits, nowInSec);
    }

    public static SinglePartitionReadCommand create(CFMetaData metadata, DecoratedKey key, PartitionFilter filter, DataLimits limits, int nowInSec)
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
