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
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.service.StorageService;

public abstract class ReadCommands
{
    private ReadCommands() {}

    public static SinglePartitionReadCommand fullPartitionRead(CFMetaData metadata, DecoratedKey key, int nowInSec)
    {
        return fullSlicesRead(metadata, key, Slices.ALL, nowInSec);
    }

    public static SinglePartitionReadCommand fullSlicesRead(CFMetaData metadata, DecoratedKey key, Slices slices, int nowInSec)
    {
        SlicePartitionFilter filter = new SlicePartitionFilter(metadata.regularColumns(),
                                                               metadata.staticColumns(),
                                                               slices,
                                                               false);
        return new SinglePartitionSliceCommand(metadata, nowInSec, ColumnFilter.NONE, DataLimits.NONE, key, filter);
    }

    public static ReadCommand allDataRead(CFMetaData metadata, int nowInSec)
    {
        return new PartitionRangeReadCommand(metadata,
                                             nowInSec,
                                             ColumnFilter.NONE,
                                             DataLimits.NONE,
                                             DataRange.allData(metadata, StorageService.getPartitioner()));
    }

    public static SinglePartitionReadCommand create(CFMetaData metadata, ByteBuffer key, PartitionFilter filter, DataLimits limits, int nowInSec)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return create(metadata, dk, filter, limits, nowInSec);
    }

    public static SinglePartitionReadCommand create(CFMetaData metadata, DecoratedKey key, PartitionFilter filter, DataLimits limits, int nowInSec)
    {
        if (filter instanceof SlicePartitionFilter)
            return new SinglePartitionSliceCommand(metadata, nowInSec, ColumnFilter.NONE, DataLimits.NONE, key, (SlicePartitionFilter)filter);
        else
            return new SinglePartitionNamesCommand(metadata, nowInSec, ColumnFilter.NONE, DataLimits.NONE, key, (NamesPartitionFilter)filter);
    }
}
