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

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.cassandra.db.marshal.BytesType;

/*
 * The layout of a marker column is:
 *   <uuid client> : <uuid server><key><counter cfid><counter name><value><leader>
 */
public class MarkerColumn extends ExpiringColumn
{
    protected static Logger logger = Logger.getLogger(MarkerColumn.class);

    public MarkerColumn(ByteBuffer uuidClient, ByteBuffer uuidServer, long value, int cfid, ByteBuffer key, ByteBuffer name, ByteBuffer leader, long timestamp, int ttl)
    {
        super(uuidClient, new Info(uuidServer, key, cfid,  name, value, leader).serialize(), timestamp, ttl);
    }

    public MarkerColumn(ByteBuffer name, ByteBuffer value, long timestamp, int ttl, int localExpirationTime)
    {
      super(name, value, timestamp, ttl, localExpirationTime);
    }

    public Info getInfos()
    {
        return Info.deserialize(value);
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        logger.info("Reconciling " + this.getString(BytesType.instance) + " and " + column.getString(BytesType.instance));

        if (column instanceof LocalMarkerColumn)
          return column.reconcile(this);

        if (!(column instanceof MarkerColumn))
            return super.reconcile(column);

        MarkerColumn mc = (MarkerColumn) column;
        Info info = getInfos();
        Info otherInfo = mc.getInfos();

        // repair for localhost are handled by LocalMarkerColumn
        if (info.leader.compareTo(SystemTable.getNodeUUID()) == 0)
          return column;
        if (otherInfo.leader.compareTo(SystemTable.getNodeUUID()) == 0)
          return this;

        logger.info("this infos  = " + info);
        logger.info("other infos = " + otherInfo);
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
            logger.info("triggering repair for " + repairInfo);
            CounterRepairManager.getManager(repairInfo.cfid).doRepair(toRepair);
            return c < 0 ? mc : this;
        }
    }

    @Override
    public int serializationFlags()
    {
        return super.serializationFlags() | ColumnSerializer.MARKER_MASK;
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append("marker");
        sb.append(":");
        sb.append(value);
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }

    public static ByteBuffer getLeader(ByteBuffer data)
    {
      return Info.deserialize(data).leader;
    }

    public static class Info
    {
        public final ByteBuffer uuidServer;
        public final ByteBuffer key;
        public final int cfid;
        public final ByteBuffer name;
        public final long value;
        public final ByteBuffer leader;

        private Info(ByteBuffer uuidServer, ByteBuffer key, int cfid, ByteBuffer name, long value, ByteBuffer leader)
        {
            this.uuidServer = uuidServer;
            this.key = key;
            this.cfid = cfid;
            this.name = name;
            this.value = value;
            this.leader = leader;
        }

        private static Info deserialize(ByteBuffer data)
        {
            ByteBuffer bb = data.duplicate();

            //logger.debug("bb = " + bb);
            //logger.debug("Reading uuidServer");
            ByteBuffer uuidServer = readShortByteArray(bb);
            //logger.debug("bb = " + bb + ", uuidServer=" + uuidServer);
            //logger.debug("Reading key");
            ByteBuffer key = readShortByteArray(bb);
            //logger.debug("bb = " + bb + ", key=" + key);
            int cfid = bb.getInt();
            ByteBuffer name = readShortByteArray(bb);
            long value = bb.getLong();
            ByteBuffer leader = readShortByteArray(bb);
            //logger.info("uuidServer=" + uuidServer + ", key=" + key + ", cfid=" + cfid + ", name=" + name + ", value=" + value + ",leader=" + leader);
            //logger.info("data = " + data);
            return new Info(uuidServer, key, cfid, name, value, leader);
        }

        private ByteBuffer serialize()
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(serializedSize());
            try
            {
                DataOutput out = new DataOutputStream(bos);
                FBUtilities.writeShortByteArray(uuidServer, out);
                FBUtilities.writeShortByteArray(key, out);
                out.writeInt(cfid);
                FBUtilities.writeShortByteArray(name, out);
                out.writeLong(value);
                FBUtilities.writeShortByteArray(leader, out);
                return ByteBuffer.wrap(bos.toByteArray());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        // Increments the position
        private static int readShortLength(ByteBuffer bb)
        {
            int length = (bb.get() & 0xFF) << 8;
            return length | (bb.get() & 0xFF);
        }

        // Increments the position
        private static ByteBuffer readShortByteArray(ByteBuffer bb)
        {
            int length = readShortLength(bb);
            ByteBuffer array = bb.slice();
            array.limit(length);
            bb.position(bb.position() + length);
            return array;
        }

        private int serializedSize()
        {
            int size = 2 + uuidServer.remaining();
            size    += 2 + key.remaining();
            size    += 4;
            size    += 2 + name.remaining();
            size    += 8;
            size    += 2 + leader.remaining();
            return size;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("[uuidServer=").append(FBUtilities.bytesToHex(uuidServer));
            builder.append(" key=").append(FBUtilities.bytesToHex(key));
            builder.append(" cfid=").append(cfid);
            builder.append(" name=").append(FBUtilities.bytesToHex(name));
            builder.append(" value=").append(value);
            builder.append(" leader=").append(FBUtilities.bytesToHex(leader));
            return builder.append("]").toString();
        }
    }
}
