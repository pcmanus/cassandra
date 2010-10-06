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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CFMetaData;

import org.apache.cassandra.db.marshal.BytesType;

public class CounterRepairManager
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutation.class);

    private static final ConcurrentHashMap<Integer, CounterRepairManager> managers = new ConcurrentHashMap<Integer, CounterRepairManager>();
    private static final Object[] repairLocks;

    static {
        repairLocks = new Object[DatabaseDescriptor.getConcurrentReaders() * 8];
        for (int i = 0; i < repairLocks.length; i++)
            repairLocks[i] = new Object();
    }

    private final int cfid;

    private CounterRepairManager(int cfid)
    {
        this.cfid = cfid;
    }

    public static CounterRepairManager getManager(int cfid)
    {
        CounterRepairManager manager = managers.get(cfid);
        if (manager == null)
        {
            manager = new CounterRepairManager(cfid);
            CounterRepairManager alreadyPresent = managers.putIfAbsent(cfid, manager);
            if (alreadyPresent != null)
            {
                manager = alreadyPresent;
            }
        }
        return manager;
    }

    public void doRepair(MarkerColumn marker)
    {
        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(cfid);
        String table = cfm.tableName;
        String metadataCF = cfm.counterMetadataCF;
        if (metadataCF == null)
        {
            logger.error("Counter update replay detected for CF " + cfm.cfName + ", but no counter metadata cf defined");
            return;
        }
        CFMetaData metadataCfm = DatabaseDescriptor.getCFMetaData(table, metadataCF);
        if (metadataCfm == null)
        {
            logger.error("Counter update replay detected for CF " + cfm.cfName + ", but counter metadata cf not properly configured");
            return;
        }

        ByteBuffer uuidClient = marker.name();
        MarkerColumn.Info markerInfo = marker.getInfos();

        synchronized (repairLockFor(uuidClient))
        {
            try
            {
                ByteBuffer repairName = makeRepairMarkerKey(uuidClient, markerInfo.uuidServer);
                Column repairMarker = read(table, metadataCF, markerInfo.key, repairName);
                logger.info("repair marker: " + (repairMarker == null ? "no" : "yes"));
                if (repairMarker == null)
                {
                    repairMarker = new ExpiringColumn(repairName, FBUtilities.EMPTY_BYTE_BUFFER, System.currentTimeMillis(), metadataCfm.gcGraceSeconds);
                    RowMutation mutation = new RowMutation(table, markerInfo.key);
                    ColumnFamily cfCounter = ColumnFamily.create(cfid);
                    ColumnFamily cfMetadata = ColumnFamily.create(table, metadataCF);
                    CounterColumn counterColumn;
                    if (markerInfo.leader.compareTo(SystemTable.getNodeUUID()) == 0)
                    {
                        counterColumn = new LocalCounterColumn(-markerInfo.value, marker.timestamp());
                    }
                    else
                    {
                        // TODO: using tstamp + 1 is not the cleaner thing ever, but I don't see good alternative right now
                        counterColumn = new CounterColumn(markerInfo.leader, markerInfo.value, marker.timestamp() + 1);
                    }
                    logger.info("Adding table=" + table + " key=" + FBUtilities.bytesToHex(markerInfo.key)+ " cf=" + cfm.cfName + " name=" + FBUtilities.bytesToHex(markerInfo.name) + " : " + counterColumn.getString(BytesType.instance));
                    cfCounter.addColumn(markerInfo.name, counterColumn);
                    cfMetadata.addColumn(repairMarker);
                    mutation.add(cfCounter);
                    mutation.add(cfMetadata);
                    logger.info("Applying mutation: " + mutation);
                    mutation.apply();
                    logger.info("Applied in repairs");
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static Column read(String table, String cfName, ByteBuffer key, ByteBuffer cname)
    throws IOException
    {
        QueryPath queryPath = new QueryPath(cfName);
        ReadCommand readCommand = new SliceByNamesReadCommand(table, key, queryPath, Collections.singletonList(cname));
        Table t = Table.open(table);
        Row row = readCommand.getRow(t);
        if (row == null || row.cf == null || row.cf.getColumnCount() == 0)
        {
            return null;
        }
        else
        {
            IColumn c = row.cf.getColumn(cname);
            if (c == null || c.isMarkedForDelete())
            {
                return null;
            }
            else
            {
                assert c instanceof Column;
                return (Column) c;
            }
        }
    }

    public static ByteBuffer makeRepairMarkerKey(ByteBuffer uuidClient, ByteBuffer uuidServer)
    {
        ByteBuffer buffer = ByteBuffer.allocate(uuidClient.remaining() + uuidServer.remaining());
        buffer.mark();
        buffer.put(uuidServer.array(), uuidServer.arrayOffset() + uuidServer.position(), uuidServer.remaining());
        // we put uuidClient last, because that's the only one for which we don't assume a specific size
        buffer.put(uuidClient.array(), uuidClient.arrayOffset() + uuidClient.position(), uuidClient.remaining());
        buffer.reset();
        return buffer;
    }

    private static Object repairLockFor(ByteBuffer uuid)
    {
        return repairLocks[Math.abs(uuid.hashCode() % repairLocks.length)];
    }
}
