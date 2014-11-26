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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static methods to work with row iterators.
 */
public abstract class RowIterators
{
    private RowIterators() {}

    /**
     * Returns whether the provided iterator has no data (including no deletion data).
     */
    public static boolean isEmpty(RowIterator iterator)
    {
        return !iterator.hasNext() && iterator.staticRow().isEmpty();
    }

    public static String toString(RowIterator iterator)
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = iterator.metadata();
        PartitionColumns columns = iterator.columns();

        sb.append(String.format("[%s.%s] key=%s columns=%s reversed=%b\n",
                                metadata.ksName,
                                metadata.cfName,
                                metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                columns,
                                iterator.isReverseOrder()));

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("-----\n").append(Rows.toString(metadata, iterator.staticRow()));

        while (iterator.hasNext())
            sb.append("-----\n").append(Rows.toString(metadata, iterator.next()));

        sb.append("-----\n");
        return sb.toString();
    }

    public static PartitionUpdate toUpdate(RowIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
