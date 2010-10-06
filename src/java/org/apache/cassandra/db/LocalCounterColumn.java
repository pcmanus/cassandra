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

import org.apache.log4j.Logger;

import org.apache.cassandra.db.marshal.BytesType;

/*
 * CounterColumn (long value), that sums the value when reconciled
 * with another such column. Not that the code assumes that such a column will
 * only be reconciled against another CounterColumn or a DeletedColumn.
 */
public class LocalCounterColumn extends CounterColumn
{
    protected static Logger logger = Logger.getLogger(LocalCounterColumn.class);

    public LocalCounterColumn(ByteBuffer value, long timestamp)
    {
        super(SystemTable.getNodeUUID(), value, timestamp);
    }

    public LocalCounterColumn(long value, long timestamp)
    {
        super(SystemTable.getNodeUUID(), value, timestamp);
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        logger.info("(localCounter) Reconciling " + this.getString(BytesType.instance) + " and " + column.getString(BytesType.instance));
        assert column instanceof CounterColumn || column instanceof DeletedColumn;

        // tombstones take precedence.
        if (column instanceof DeletedColumn)
            return timestamp() > column.timestamp() ? this : column;

        // if merging a CounterColumn with a LocalCounterColumn, we return
        // the LocalCounterColumn
        if (!(column instanceof LocalCounterColumn))
            return column;

        long total = getCount() + ((LocalCounterColumn) column).getCount();
        return new LocalCounterColumn(total, Math.max(timestamp(), column.timestamp()));
    }

    @Override
    public int serializationFlags()
    {
        return super.serializationFlags() | ColumnSerializer.LOCAL_MASK;
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append("local_counter");
        sb.append(":");
        sb.append(getCount());
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }
}
