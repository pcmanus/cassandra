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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.thrift.ConsistencyLevel;

import org.apache.cassandra.db.marshal.BytesType;

public class CounterMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutation.class);
    private static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private final String table;
    private final ByteBuffer key;
    private final int cfid;
    private final ByteBuffer name;
    private long value;
    private final long timestamp;

    private final ConsistencyLevel consistency;

    private final ByteBuffer uuidClient;  // an id that uniquely identify this update
    private ByteBuffer uuidServer;
    private long repairValue;

    public CounterMutation(String table, ByteBuffer key, int cfid, ByteBuffer name, long value, long timestamp, ConsistencyLevel consistency, ByteBuffer uuid)
    {
        this.table = table;
        this.key = key;
        this.cfid = cfid;
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
        this.consistency = consistency;

        this.uuidClient = uuid == null ? FBUtilities.EMPTY_BYTE_BUFFER : uuid;
    }

    public String getTable()
    {
        return table;
    }

    public ByteBuffer key()
    {
        return key;
    }

    public ByteBuffer name()
    {
        return name;
    }

    public int cfid()
    {
        return cfid;
    }

    public long value()
    {
        return value;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public ByteBuffer uuid()
    {
        return uuidClient;
    }

    public boolean hasUuid()
    {
        return uuidClient.remaining() != 0;
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public static CounterMutationSerializer serializer()
    {
        return serializer;
    }

    public RowMutation makeReplicationMutation() throws IOException
    {
        RowMutation mutation = new RowMutation(table, key);
        ColumnFamily cfCounter = ColumnFamily.create(cfid);
        cfCounter.addColumn(name, new CounterColumn(SystemTable.getNodeUUID(), value, timestamp));
        if (hasUuid())
        {
            CFMetaData cfm = DatabaseDescriptor.getCFMetaData(cfid);
            if (cfm.counterMetadataCF != null)
            {
                CFMetaData metadataCF = DatabaseDescriptor.getCFMetaData(table, cfm.counterMetadataCF);
                if (metadataCF != null)
                {
                    ColumnFamily cfMetadata = ColumnFamily.create(metadataCF.cfId);
                    cfMetadata.addColumn(new MarkerColumn(uuidClient, uuidServer, repairValue, cfid, key, name, SystemTable.getNodeUUID(), timestamp, metadataCF.gcGraceSeconds));
                    mutation.add(cfMetadata);
                }
            }
        }
        mutation.add(cfCounter);
        return mutation;
    }

    public Message makeMutationMessage() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), StorageService.Verb.COUNTER_UPDATE, bos.toByteArray());
    }

    /*
     * Apply this CounterMutation locally.
     */
    public void apply() throws IOException
    {
        RowMutation mutation = new RowMutation(table, key);
        ColumnFamily cfCounter = ColumnFamily.create(cfid);
        cfCounter.addColumn(name, new LocalCounterColumn(value, timestamp));
        if (hasUuid())
        {
            CFMetaData cfm = DatabaseDescriptor.getCFMetaData(cfid);
            if (cfm.counterMetadataCF != null)
            {
                CFMetaData metadataCF = DatabaseDescriptor.getCFMetaData(table, cfm.counterMetadataCF);
                if (metadataCF != null)
                {
                    uuidServer =  ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress())));
                    ColumnFamily cfMetadata = ColumnFamily.create(metadataCF.cfId);
                    cfMetadata.addColumn(new LocalMarkerColumn(uuidClient, uuidServer, value, cfid, key, name, SystemTable.getNodeUUID(), timestamp, metadataCF.gcGraceSeconds));
                    mutation.add(cfMetadata);
                }
            }
        }
        mutation.add(cfCounter);
        logger.info("mutation = " + mutation);
        mutation.apply();
        logger.info("applied");
    }

    private CounterColumn readLocalCounterColumn() throws IOException
    {
        String cfName = DatabaseDescriptor.getCFMetaData(cfid).cfName;
        QueryPath queryPath = new QueryPath(cfName, name);
        ReadCommand readCommand = new SliceByNamesReadCommand(table, key, queryPath, Collections.singletonList(SystemTable.getNodeUUID()));
        Table t = Table.open(readCommand.table);
        Row row = readCommand.getRow(t);
        if (row == null || row.cf == null || row.cf.getColumnCount() == 0)
        {
            return null;
        }
        else
        {
            IColumn c = row.cf.getColumn(SystemTable.getNodeUUID());
            if (c == null || c.isMarkedForDelete())
            {
                return null;
            }
            else
            {
                assert c instanceof CounterColumn;
                return (CounterColumn) c;
            }
        }
    }

    public void updateCount() throws IOException
    {
      CounterColumn localCounter = readLocalCounterColumn();
      if (localCounter != null)
      {
        repairValue = localCounter.getCount() - value;
        value = localCounter.getCount();
      }
    }

    @Override
    public String toString()
    {
        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(cfid);
        StringBuilder buff = new StringBuilder("CounterMutation(");
        buff.append("keyspace='").append(table).append('\'');
        buff.append(", key='").append(FBUtilities.bytesToHex(key)).append('\'');
        buff.append(", cf='").append(cfm == null ? "-dropped-" : cfm.cfName).append('\'');
        buff.append(", name='").append(FBUtilities.bytesToHex(name)).append('\'');
        buff.append(", value='").append(value).append('\'');
        buff.append(", timestamp='").append(timestamp).append('\'');
        buff.append(", consistency='").append(consistency.toString()).append('\'');
        if (hasUuid())
            buff.append(", uuid='").append(FBUtilities.bytesToHex(uuidClient)).append('\'');
        if (uuidServer != null)
            buff.append(", uuidServer='").append(FBUtilities.bytesToHex(uuidServer)).append('\'');
        return buff.append(")").toString();
    }

    public String toString(boolean shallow)
    {
        return toString();
    }
}

class CounterMutationSerializer implements ICompactSerializer<CounterMutation>
{
    public void serialize(CounterMutation cm, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(cm.getTable());
        FBUtilities.writeShortByteArray(cm.key(), dos);
        FBUtilities.writeShortByteArray(cm.name(), dos);
        dos.writeInt(cm.cfid());
        dos.writeLong(cm.value());
        dos.writeLong(cm.timestamp());
        dos.writeUTF(cm.consistency().name());
        FBUtilities.writeShortByteArray(cm.uuid(), dos);
    }

    public CounterMutation deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        ByteBuffer key = FBUtilities.readShortByteArray(dis);
        ByteBuffer name = FBUtilities.readShortByteArray(dis);
        int cfid = dis.readInt();
        long value = dis.readLong();
        long timestamp = dis.readLong();
        ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, dis.readUTF());
        ByteBuffer uuid = FBUtilities.readShortByteArray(dis);
        return new CounterMutation(table, key, cfid, name, value, timestamp, consistency, uuid);
    }
}
