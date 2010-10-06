/**
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

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.marshal.AbstractType;

/*
 * A CounterColumn is a run-of-the-mill Column, but one that we assume holds a
 * counter (long value). Other than that, it has no special capability.
 */
public class CounterColumn extends Column
{
    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    public CounterColumn(ByteBuffer name, long value, long timestamp)
    {
        super(name, FBUtilities.toByteBuffer(value), timestamp);
    }

    public long getCount()
    {
        return value.getLong(value.position());
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        // if merging a CounterColumn with a LocalCounterColumn, we return
        // the LocalCounterColumn
        if (column instanceof LocalCounterColumn)
            return column;

        return super.reconcile(column);
    }

    @Override
    public int serializationFlags()
    {
        return super.serializationFlags() | ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append("counter");
        sb.append(":");
        sb.append(getCount());
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }
}
