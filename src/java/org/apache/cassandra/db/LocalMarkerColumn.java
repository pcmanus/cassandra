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

import org.apache.cassandra.db.marshal.BytesType;

public class LocalMarkerColumn extends MarkerColumn
{
    public LocalMarkerColumn(ByteBuffer uuidClient, ByteBuffer uuidServer, long value, int cfid, ByteBuffer key, ByteBuffer name, ByteBuffer leader, long timestamp, int ttl)
    {
        super(uuidClient, uuidServer, value, cfid, key, name, leader, timestamp, ttl);
    }

    public LocalMarkerColumn(ByteBuffer name, ByteBuffer value, long timestamp, int ttl, int localExpirationTime)
    {
      super(name, value, timestamp, ttl, localExpirationTime);
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        logger.info("(localMarker) Reconciling " + this.getString(BytesType.instance) + " and " + column.getString(BytesType.instance));

        if (!(column instanceof MarkerColumn))
            return super.reconcile(column);

        MarkerColumn mc = (MarkerColumn) column;
        Info info = getInfos();
        Info otherInfo = mc.getInfos();

        //logger.info("this infos  = " + info);
        //logger.info("other infos = " + otherInfo);

        // ignore non LocalMarkerColumn locally
        if (!(column instanceof LocalMarkerColumn) && otherInfo.leader.compareTo(SystemTable.getNodeUUID()) == 0)
          return this;


        int c = info.uuidServer.compareTo(otherInfo.uuidServer);
        if (c == 0)
        {
            assert this.equals(column);
            return this;
        }
        else
        {
            MarkerColumn toRepair = c < 0 ? this : mc;
            Info repairInfo = c < 0 ? info : otherInfo;
            CounterRepairManager.getManager(repairInfo.cfid).doRepair(toRepair);
            return c < 0 ? mc : this;
        }
    }

    @Override
    public int serializationFlags()
    {
        return super.serializationFlags() | ColumnSerializer.LOCAL_MASK;
    }

    // don't take local mask into account for digests
    @Override
    public int serializationFlagsForDigest()
    {
        return super.serializationFlags();
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append("local_marker");
        sb.append(":");
        sb.append(value);
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }
}
