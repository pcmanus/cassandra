package org.apache.cassandra.service;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.SinglePartitionNamesReadBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.atoms.Rows;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.assertClustering;
import static org.apache.cassandra.Util.assertColumn;
import static org.apache.cassandra.Util.assertColumns;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataResolverTest
{
    public static final String KEYSPACE1 = "DataResolverTest";
    public static final String CF_STANDARD = "Standard1";

    // counter to generate the last byte of the respondent's address in a ReadResponse message
    private int addressSuffix = 10;

    private DecoratedKey dk;
    private Keyspace ks;
    private ColumnFamilyStore cfs;
    private CFMetaData cfm;
    private int nowInSeconds;
    private ReadCommand command;
    private MessageRecorder messageRecorder;


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CFMetaData cfMetadata = CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD)
                                                  .addPartitionKey("key", BytesType.instance)
                                                  .addClusteringColumn("col1", AsciiType.instance)
                                                  .addRegularColumn("c1", AsciiType.instance)
                                                  .addRegularColumn("c2", AsciiType.instance)
                                                  .addRegularColumn("one", AsciiType.instance)
                                                  .addRegularColumn("two", AsciiType.instance)
                                                  .build();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    cfMetadata);
    }

    @Before
    public void setup()
    {
        dk = Util.dk("key1");
        ks = Keyspace.open(KEYSPACE1);
        cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata;
        nowInSeconds = FBUtilities.nowInSeconds();
        command = new SinglePartitionNamesReadBuilder(cfs, nowInSeconds, dk).addClustering().build();
    }

    @Before
    public void injectMessageSink()
    {
        // install an IMessageSink to capture all messages
        // so we can inspect them during tests
        messageRecorder = new MessageRecorder();
        MessagingService.instance().addMessageSink(messageRecorder);
    }

    @After
    public void removeMessageSink()
    {
        // should be unnecessary, but good housekeeping
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void testResolveNewerSingleRow() throws UnknownHostException
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c1", "v1", 0)
                                                                                         .build()));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c1", "v2", 1)
                                                                                         .build()));

        try(DataIterator data = resolver.resolve();
            RowIterator rows = getOnlyRowIterator(data))
        {
            Row row = getOnlyRow(rows);
            assertColumns(row, "c1");
            assertColumn(cfm, row, "c1", "v2", 1);
        }

        assertEquals(1, messageRecorder.sent.size());
        // peer 1 just needs to repair with the row from peer 2
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v2", 1);
    }


    @Test
    public void testResolveDisjointSingleRow()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c1", "v1", 0)
                                                                                         .build()));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c2", "v2", 1)
                                                                                         .build()));

        try(DataIterator data = resolver.resolve();
            RowIterator rows = getOnlyRowIterator(data))
        {
            Row row = getOnlyRow(rows);
            assertColumns(row, "c1", "c2");
            assertColumn(cfm, row, "c1", "v1", 0);
            assertColumn(cfm, row, "c2", "v2", 1);
        }

        assertEquals(2, messageRecorder.sent.size());
        // each peer needs to repair with each other's column
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsColumn(msg, "1", "c2", "v2", 1);

        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRows() throws UnknownHostException
    {

        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c1", "v1", 0)
                                                                                         .build()));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iterBuilder(cfm, dk, nowInSeconds).clustering("2")
                                                                                         .add("c2", "v2", 1)
                                                                                         .build()));

        try(DataIterator data = resolver.resolve())
        {
            RowIterator rows = data.next();
            // We expect the resolved superset to contain both rows
            Row row = rows.next();
            assertClustering(cfm, row, "1");
            assertColumns(row, "c1");
            assertColumn(cfm, row, "c1", "v1", 0);

            row = rows.next();
            assertClustering(cfm, row, "2");
            assertColumns(row, "c2");
            assertColumn(cfm, row, "c2", "v2", 1);

            assertFalse(rows.hasNext());
            assertFalse(data.hasNext());
        }

        assertEquals(2, messageRecorder.sent.size());
        // each peer needs to repair the row from the other
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "2", "c2", "v2", 1);

        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRowsWithRangeTombstones()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4);
        RangeTombstone tombstone1 = tombstone("1", "11", 1, nowInSeconds);
        RangeTombstone tombstone2 = tombstone("3", "31", 1, nowInSeconds);

        InetAddress peer1 = peer();
        PartitionIterator iter1 = iterBuilder(cfm, dk, nowInSeconds).addRangeTombstone(tombstone1)
                                                                    .addRangeTombstone(tombstone2)
                                                                    .build();
        resolver.preprocess(readResponseMessage(peer1, iter1));
        // not covered by any range tombstone
        InetAddress peer2 = peer();
        PartitionIterator iter2 = iterBuilder(cfm, dk, nowInSeconds).clustering("0")
                                                                    .add("c1", "v0", 0)
                                                                    .build();
        resolver.preprocess(readResponseMessage(peer2, iter2));
        // covered by a range tombstone
        InetAddress peer3 = peer();
        PartitionIterator iter3 = iterBuilder(cfm, dk, nowInSeconds).clustering("10")
                                                                    .add("c2", "v1", 0)
                                                                    .build();
        resolver.preprocess(readResponseMessage(peer3, iter3));
        // range covered by rt, but newer
        InetAddress peer4 = peer();
        PartitionIterator iter4 = iterBuilder(cfm, dk, nowInSeconds).clustering("3")
                                                                    .add("one", "A", 2)
                                                                    .build();
        resolver.preprocess(readResponseMessage(peer4, iter4));
        try(DataIterator data = resolver.resolve())
        {
            RowIterator rows = data.next();

            Row row = rows.next();
            assertClustering(cfm, row, "0");
            assertColumns(row, "c1");
            assertColumn(cfm, row, "c1", "v0", 0);

            row = rows.next();
            assertClustering(cfm, row, "3");
            assertColumns(row, "one");
            assertColumn(cfm, row, "one", "A", 2);
        }

        assertEquals(4, messageRecorder.sent.size());
        // peer1 needs the rows from peers 2 and 4
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer2 needs to get the row from peer4 and the RTs
        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer 3 needs both rows and the RTs
        msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer4 needs the row from peer2  and the RTs
        msg = getSentMessage(peer4);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
    }

    @Test
    public void testResolveWithOneEmpty()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("c2", "v2", 1)
                                                                                         .build()));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, PartitionIterators.EMPTY));

        try(DataIterator data = resolver.resolve();
            RowIterator rows = getOnlyRowIterator(data))
        {
            Row row = getOnlyRow(rows);
            assertColumns(row, "c2");
            assertColumn(cfm, row, "c2", "v2", 1);
        }

        assertEquals(1, messageRecorder.sent.size());
        // peer 2 needs the row from peer 1
        MessageOut msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c2", "v2", 1);
    }

    @Test
    public void testResolveWithBothEmpty()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        resolver.preprocess(readResponseMessage(peer(), PartitionIterators.EMPTY));
        resolver.preprocess(readResponseMessage(peer(), PartitionIterators.EMPTY));

        try(DataIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        assertTrue(messageRecorder.sent.isEmpty());
    }

    @Test
    public void testResolveDeleted()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        // one response with columns timestamped before a delete in another response
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("one", "A", 0)
                                                                                         .build()));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, fullPartitionDelete(cfm, dk, 1, nowInSeconds)));

        try(DataIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        // peer1 should get the deletion from peer2
        assertEquals(1, messageRecorder.sent.size());
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new SimpleDeletionTime(1, nowInSeconds));
        assertRepairContainsNoColumns(msg);
    }

    @Test
    public void testResolveMultipleDeleted()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4);
        // deletes and columns with interleaved timestamp, with out of order return sequence
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, fullPartitionDelete(cfm, dk, 0, nowInSeconds)));
        // these columns created after the previous deletion
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("one", "A", 1)
                                                                                         .add("two", "A", 1)
                                                                                         .build()));
        //this column created after the next delete
        InetAddress peer3 = peer();
        resolver.preprocess(readResponseMessage(peer3, iterBuilder(cfm, dk, nowInSeconds).clustering("1")
                                                                                         .add("two", "B", 3)
                                                                                         .build()));
        InetAddress peer4 = peer();
        resolver.preprocess(readResponseMessage(peer4, fullPartitionDelete(cfm, dk, 2, nowInSeconds)));

        try(DataIterator data = resolver.resolve();
            RowIterator rows = getOnlyRowIterator(data))
        {
            Row row = getOnlyRow(rows);
            assertColumns(row, "two");
            assertColumn(cfm, row, "two", "B", 3);
        }

        // peer 1 needs to get the partition delete from peer 4 and the row from peer 3
        assertEquals(4, messageRecorder.sent.size());
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new SimpleDeletionTime(2, nowInSeconds));
        assertRepairContainsColumn(msg, "1", "two", "B", 3);

        // peer 2 needs the deletion from peer 4 and the row from peer 3
        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new SimpleDeletionTime(2, nowInSeconds));
        assertRepairContainsColumn(msg, "1", "two", "B", 3);

        // peer 3 needs just the deletion from peer 4
        msg = getSentMessage(peer3);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new SimpleDeletionTime(2, nowInSeconds));
        assertRepairContainsNoColumns(msg);

        // peer 4 needs just the row from peer 3
        msg = getSentMessage(peer4);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "two", "B", 3);
    }

    private InetAddress peer()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix++ });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private MessageOut<Mutation> getSentMessage(InetAddress target)
    {
        MessageOut<Mutation> message = messageRecorder.sent.get(target);
        assertNotNull(String.format("No repair message was sent to %s", target), message);
        return message;
    }

    private void assertRepairContainsDeletions(MessageOut<Mutation> message,
                                               DeletionTime deletionTime,
                                               RangeTombstone...rangeTombstones)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        DeletionInfo deletionInfo = update.deletionInfo();
        if (deletionTime != null)
            assertEquals(deletionTime, deletionInfo.getPartitionDeletion());

        assertEquals(rangeTombstones.length, deletionInfo.rangeCount());
        Iterator<RangeTombstone> ranges = deletionInfo.rangeIterator(false);
        int i = 0;
        while(ranges.hasNext())
        {
            assertEquals(ranges.next(), rangeTombstones[i++]);
        }
    }

    private void assertRepairContainsNoDeletions(MessageOut<Mutation> message)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertTrue(update.deletionInfo().isLive());
    }

    private void assertRepairContainsColumn(MessageOut<Mutation> message,
                                            String clustering,
                                            String columnName,
                                            String value,
                                            long timestamp)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        Row row = update.iterator().next();
        assertClustering(cfm, row, clustering);
        assertColumn(cfm, row, columnName, value, timestamp);
    }

    private void assertRepairContainsNoColumns(MessageOut<Mutation> message)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertFalse(update.iterator().hasNext());
    }

    private void assertRepairMetadata(MessageOut<Mutation> message)
    {
        assertEquals(MessagingService.Verb.READ_REPAIR, message.verb);
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertEquals(update.metadata().ksName, cfm.ksName);
        assertEquals(update.metadata().cfName, cfm.cfName);
    }

    public MessageIn<ReadResponse> readResponseMessage(InetAddress from, PartitionIterator partitionIterator)
    {
        return MessageIn.create(from,
                                ReadResponse.createDataResponse(partitionIterator),
                                Collections.EMPTY_MAP,
                                MessagingService.Verb.REQUEST_RESPONSE,
                                MessagingService.current_version);
    }

    private PartitionIterator fullPartitionDelete(CFMetaData cfm, DecoratedKey dk, long timestamp, int nowInSeconds)
    {
        return new SingletonPartitionIterator(
                         PartitionUpdate.fullPartitionDelete(cfm, dk, timestamp, nowInSeconds).atomIterator());
    }

    private RowIterator getOnlyRowIterator(DataIterator data)
    {
        assertTrue("DataIterator is empty, expected a single RowIterator", data.hasNext());
        RowIterator rows = data.next();
        assertFalse("DataIterator contains multiple RowIterators, expected only one", data.hasNext());
        return rows;
    }

    private Row getOnlyRow(RowIterator rows)
    {
        assertTrue("RowIterator is empty, expected a single Row", rows.hasNext());
        Row row = rows.next();
        assertFalse("RowIterator contains multiple Rows, expected only one", rows.hasNext());
        return row;
    }

    private RangeTombstone tombstone(Object start, Object end, long markedForDeleteAt, int localDeletionTime)
    {
        return new RangeTombstone(Slice.make(cfm.comparator, cfm.comparator.make(start), cfm.comparator.make(end)),
                                  new SimpleDeletionTime(markedForDeleteAt, localDeletionTime));
    }

    private static class MessageRecorder implements IMessageSink
    {
        Map<InetAddress, MessageOut> sent = new HashMap<>();
        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
        {
            sent.put(to, message);
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return false;
        }
    }

    private SingletonPartitionIteratorBuilder iterBuilder(CFMetaData cfm, DecoratedKey dk, int nowInSeconds)
    {
        return new SingletonPartitionIteratorBuilder(cfm, dk, nowInSeconds);
    }

    private static class SingletonPartitionIteratorBuilder
    {
        private final CFMetaData cfm;
        private final PartitionUpdate update;

        public SingletonPartitionIteratorBuilder(CFMetaData cfm, DecoratedKey dk, int nowInSeconds)
        {
            this.cfm = cfm;
            update = new PartitionUpdate(cfm, dk, cfm.partitionColumns(), 1, nowInSeconds);
        }

        public SingletonPartitionIteratorBuilder clustering(Object... clusteringValues)
        {
            assert clusteringValues.length == update.metadata().comparator.size();
            if (clusteringValues.length > 0)
                Rows.writeClustering(update.metadata().comparator.make(clusteringValues), update.writer());
            return this;
        }

        public SingletonPartitionIteratorBuilder add(String name, Object value, long timestamp)
        {
            ColumnDefinition def = cfm.getColumnDefinition(new ColumnIdentifier(name, true));
            update.writer().writeCell(def,
                                      false,
                                      ((AbstractType) def.cellValueType()).decompose(value),
                                      SimpleLivenessInfo.forUpdate(timestamp,
                                                                   LivenessInfo.NO_TTL,
                                                                   LivenessInfo.NO_DELETION_TIME,
                                                                   cfm),
                                      null);
            return this;
        }

        public SingletonPartitionIteratorBuilder addRangeTombstone(RangeTombstone tombstone)
        {
            update.addRangeTombstone(tombstone);
            return this;
        }

        public PartitionIterator build()
        {
            update.writer().endOfRow();
            return new SingletonPartitionIterator(update.atomIterator());
        }
    }
}
