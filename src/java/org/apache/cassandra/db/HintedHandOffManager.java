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

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

/**
 * The hint schema looks like this:
 *
 * CREATE TABLE hints (
 *   target_id uuid,
 *   hint_id timeuuid,
 *   message_version int,
 *   mutation blob,
 *   PRIMARY KEY (target_id, hint_id, message_version)
 * ) WITH COMPACT STORAGE;
 *
 * Thus, for each node in the cluster we treat its uuid as the partition key; each hint is a logical row
 * (physical composite column) containing the mutation to replay and associated metadata.
 *
 * When FailureDetector signals that a node that was down is back up, we page through
 * the hinted mutations and send them over one at a time, waiting for
 * hinted_handoff_throttle_delay in between each.
 *
 * deliverHints is also exposed to JMX so it can be run manually if FD ever misses
 * its cue somehow.
 */

public class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";
    public static final HintedHandOffManager instance = new HintedHandOffManager();

    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);

    private static final int MAX_SIMULTANEOUSLY_REPLAYED_HINTS = 128;
    private static final int LARGE_NUMBER = 65536; // 64k nodes ought to be enough for anybody.

    public final HintedHandoffMetrics metrics = new HintedHandoffMetrics();

    private volatile boolean hintedHandOffPaused = false;

    static final int maxHintTTL = Integer.parseInt(System.getProperty("cassandra.maxHintTTL", String.valueOf(Integer.MAX_VALUE)));

    private final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<InetAddress>();

    private final ThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getMaxHintsThread(),
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.SECONDS,
                                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                                 new NamedThreadFactory("HintedHandoff", Thread.MIN_PRIORITY),
                                                                                 "internal");

    private final ColumnFamilyStore hintStore = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF);

    private static final ColumnDefinition hintColumn = CFMetaData.HintsCf.compactValueColumn();

    /**
     * Returns a mutation representing a Hint to be sent to <code>targetId</code>
     * as soon as it becomes available again.
     */
    public Mutation hintFor(Mutation mutation, long now, int ttl, UUID targetId)
    {
        assert ttl > 0;

        InetAddress endpoint = StorageService.instance.getTokenMetadata().getEndpointForHostId(targetId);
        // during tests we may not have a matching endpoint, but this would be unexpected in real clusters
        if (endpoint != null)
            metrics.incrCreatedHints(endpoint);
        else
            logger.warn("Unable to find matching endpoint for target {} when storing a hint", targetId);

        UUID hintId = UUIDGen.getTimeUUID();
        // serialize the hint with id and version as a composite column name

        PartitionUpdate upd = new PartitionUpdate(CFMetaData.HintsCf, StorageService.getPartitioner().decorateKey(UUIDType.instance.decompose(targetId)));
        RowUpdate row = RowUpdates.create(CFMetaData.HintsCf.comparator.make(hintId, MessagingService.current_version), Columns.of(hintColumn));

        ByteBuffer value = ByteBuffer.wrap(FBUtilities.serialize(mutation, Mutation.serializer, MessagingService.current_version));
        row.addCell(hintColumn, Cells.create(value, now, ttl, CFMetaData.HintsCf));

        return new Mutation(upd.add(row));
    }

    /*
     * determine the TTL for the hint Mutation
     * this is set at the smallest GCGraceSeconds for any of the CFs in the RM
     * this ensures that deletes aren't "undone" by delivery of an old hint
     */
    public static int calculateHintTTL(Mutation mutation)
    {
        int ttl = maxHintTTL;
        for (PartitionUpdate upd : mutation.getPartitionUpdates())
            ttl = Math.min(ttl, upd.metadata().getGcGraceSeconds());
        return ttl;
    }


    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("Created HHOM instance, registered MBean.");

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                scheduleAllDeliveries();
                metrics.log();
            }
        };
        StorageService.optionalTasks.scheduleWithFixedDelay(runnable, 10, 10, TimeUnit.MINUTES);
    }

    private static void deleteHint(ByteBuffer tokenBytes, ClusteringPrefix clustering, long timestamp)
    {
        DecoratedKey dk =  StorageService.getPartitioner().decorateKey(tokenBytes);
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, dk);
        RowUpdate upd = RowUpdates.create(CFMetaData.HintsCf, clustering)
                        .addCell(hintColumn, Cells.createTombsone(timestamp));

        mutation.addOrGet(SystemKeyspace.HINTS_CF).add(upd);
        mutation.applyUnsafe(); // don't bother with commitlog since we're going to flush as soon as we're done with delivery
    }

    public void deleteHintsForEndpoint(final String ipOrHostname)
    {
        try
        {
            InetAddress endpoint = InetAddress.getByName(ipOrHostname);
            deleteHintsForEndpoint(endpoint);
        }
        catch (UnknownHostException e)
        {
            logger.warn("Unable to find {}, not a hostname or ipaddr of a node", ipOrHostname);
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(final InetAddress endpoint)
    {
        if (!StorageService.instance.getTokenMetadata().isMember(endpoint))
            return;
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(endpoint);
        DecoratedKey dk =  StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(UUIDGen.decompose(hostId)));
        final Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, dk);
        mutation.delete(SystemKeyspace.HINTS_CF, System.currentTimeMillis());

        // execute asynchronously to avoid blocking caller (which may be processing gossip)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Deleting any stored hints for {}", endpoint);
                    mutation.apply();
                    compact();
                }
                catch (Exception e)
                {
                    logger.warn("Could not delete hints for {}: {}", endpoint, e);
                }
            }
        };
        StorageService.optionalTasks.submit(runnable);
    }

    //foobar
    public void truncateAllHints() throws ExecutionException, InterruptedException
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Truncating all stored hints.");
                    Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF).truncateBlocking();
                }
                catch (Exception e)
                {
                    logger.warn("Could not truncate all hints.", e);
                }
            }
        };
        StorageService.optionalTasks.submit(runnable).get();

    }

    @VisibleForTesting
    protected Future<?> compact()
    {
        hintStore.forceBlockingFlush();
        ArrayList<Descriptor> descriptors = new ArrayList<Descriptor>();
        for (SSTable sstable : hintStore.getDataTracker().getUncompactingSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(hintStore, descriptors, (int) (System.currentTimeMillis() / 1000));
    }

    private int waitForSchemaAgreement(InetAddress endpoint) throws TimeoutException
    {
        Gossiper gossiper = Gossiper.instance;
        int waited = 0;
        // first, wait for schema to be gossiped.
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA) == null)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Didin't receive gossiped schema from " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        waited = 0;
        // then wait for the correct schema version.
        // usually we use DD.getDefsVersion, which checks the local schema uuid as stored in the system keyspace.
        // here we check the one in gossip instead; this serves as a canary to warn us if we introduce a bug that
        // causes the two to diverge (see CASSANDRA-2946)
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && !gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA).value.equals(
                gossiper.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress()).getApplicationState(ApplicationState.SCHEMA).value))
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Could not reach schema agreement with " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        logger.debug("schema for {} matches local schema", endpoint);
        return waited;
    }

    private void deliverHintsToEndpoint(InetAddress endpoint)
    {
        if (hintStore.isEmpty())
            return; // nothing to do, don't confuse users by logging a no-op handoff

        // check if hints delivery has been paused
        if (hintedHandOffPaused)
        {
            logger.debug("Hints delivery process is paused, aborting");
            return;
        }

        logger.debug("Checking remote({}) schema before delivering hints", endpoint);
        try
        {
            waitForSchemaAgreement(endpoint);
        }
        catch (TimeoutException e)
        {
            return;
        }

        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.debug("Endpoint {} died before hint delivery, aborting", endpoint);
            return;
        }

        doDeliverHintsToEndpoint(endpoint);
    }

    private boolean checkDelivered(List<WriteResponseHandler> handlers)
    {
        for (WriteResponseHandler handler : handlers)
        {
            try
            {
                handler.get();
            }
            catch (WriteTimeoutException e)
            {
                return false;
            }
        }
        return true;
    }

    /*
     * 1. Get the key of the endpoint we need to handoff
     * 2. For each column, deserialize the mutation and send it to the endpoint
     * 3. Delete the subcolumn if the write was successful
     * 4. Force a flush
     * 5. Do major compaction to clean up all deletes etc.
     */
    private void doDeliverHintsToEndpoint(InetAddress endpoint)
    {
        // find the hints for the node using its token.
        UUID hostId = Gossiper.instance.getHostId(endpoint);
        logger.info("Started hinted handoff for host: {} with IP: {}", hostId, endpoint);
        final ByteBuffer hostIdBytes = ByteBuffer.wrap(UUIDGen.decompose(hostId));
        DecoratedKey epkey =  StorageService.getPartitioner().decorateKey(hostIdBytes);

        final AtomicInteger rowsReplayed = new AtomicInteger(0);

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (CASSANDRA-5272).
        int throttleInKB = DatabaseDescriptor.getHintedHandoffThrottleInKB()
                           / (StorageService.instance.getTokenMetadata().getAllEndpoints().size() - 1);
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        int nowInSec = FBUtilities.nowInSeconds();
        AtomIterator atomIter = ReadCommands.fullPartitionRead(CFMetaData.HintsCf, epkey, nowInSec).queryMemtableAndDisk(hintStore);
        RowIterator iter = AtomIterators.asRowIterator(atomIter, nowInSec);

        List<WriteResponseHandler> responseHandlers = Lists.newArrayList();

        while (iter.hasNext())
        {
            // check if node is still alive and we should continue delivery process
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                logger.info("Endpoint {} died during hint delivery; aborting ({} delivered)", endpoint, rowsReplayed);
                break;
            }

            // check if hints delivery has been paused during the process
            if (hintedHandOffPaused)
            {
                logger.debug("Hints delivery process is paused, aborting");
                break;
            }

            // Wait regularly on the endpoint acknowledgment. If we timeout on it,
            // the endpoint is probably dead so stop delivery
            if (responseHandlers.size() > MAX_SIMULTANEOUSLY_REPLAYED_HINTS && !checkDelivered(responseHandlers))
            {
                logger.info("Timed out replaying hints to {}; aborting ({} delivered)", endpoint, rowsReplayed);
                break;
            }

            final Row hint = iter.next();
            int version = Int32Type.instance.compose(hint.clustering().get(1));
            final Cell cell = Rows.getCell(hint, hintColumn);

            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(cell.value()));
            Mutation mutation;
            try
            {
                mutation = Mutation.serializer.deserialize(in, version);
            }
            catch (UnknownColumnFamilyException e)
            {
                logger.debug("Skipping delivery of hint for deleted table", e);
                deleteHint(hostIdBytes, hint.clustering(), cell.timestamp());
                continue;
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }

            for (UUID cfId : mutation.getColumnFamilyIds())
            {
                if (cell.timestamp() <= SystemKeyspace.getTruncatedAt(cfId))
                {
                    logger.debug("Skipping delivery of hint for truncated table {}", cfId);
                    mutation = mutation.without(cfId);
                }
            }

            if (mutation.isEmpty())
            {
                deleteHint(hostIdBytes, hint.clustering(), cell.timestamp());
                continue;
            }

            MessageOut<Mutation> message = mutation.createMessage();
            rateLimiter.acquire(message.serializedSize(MessagingService.current_version));
            Runnable callback = new Runnable()
            {
                public void run()
                {
                    rowsReplayed.incrementAndGet();
                    deleteHint(hostIdBytes, hint.clustering(), cell.timestamp());
                }
            };
            WriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.SIMPLE, callback);
            MessagingService.instance().sendRR(message, endpoint, responseHandler, false);
            responseHandlers.add(responseHandler);
        }

        // Wait on the last handlers
        if (!checkDelivered(responseHandlers))
            logger.info("Timed out replaying hints to {}; aborting ({} delivered)", endpoint, rowsReplayed);

        if (!iter.hasNext() || rowsReplayed.get() >= DatabaseDescriptor.getTombstoneWarnThreshold())
        {
            try
            {
                compact().get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Attempt delivery to any node for which we have hints.  Necessary since we can generate hints even for
     * nodes which are never officially down/failed.
     */
    private void scheduleAllDeliveries()
    {
        if (logger.isDebugEnabled())
          logger.debug("Started scheduleAllDeliveries");

        ReadCommand cmd = new PartitionRangeReadCommand(hintStore.metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        ColumnFilter.NONE,
                                                        DataLimits.cqlLimits(Integer.MAX_VALUE, 1, true),
                                                        DataRange.allData(hintStore.metadata, StorageService.getPartitioner()));

        try (PartitionIterator iter = cmd.executeLocally(hintStore))
        {
            while (iter.hasNext())
            {
                try (AtomIterator partition = iter.next())
                {
                    UUID hostId = UUIDGen.getUUID(partition.partitionKey().getKey());
                    InetAddress target = StorageService.instance.getTokenMetadata().getEndpointForHostId(hostId);
                    // token may have since been removed (in which case we have just read back a tombstone)
                    if (target != null)
                        scheduleHintDelivery(target);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        if (logger.isDebugEnabled())
          logger.debug("Finished scheduleAllDeliveries");
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void scheduleHintDelivery(final InetAddress to)
    {
        // We should not deliver hints to the same host in 2 different threads
        if (!queuedDeliveries.add(to))
            return;

        logger.debug("Scheduling delivery of Hints to {}", to);

        executor.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    deliverHintsToEndpoint(to);
                }
                finally
                {
                    queuedDeliveries.remove(to);
                }
            }
        });
    }

    public void scheduleHintDelivery(String to) throws UnknownHostException
    {
        scheduleHintDelivery(InetAddress.getByName(to));
    }

    public void pauseHintsDelivery(boolean b)
    {
        hintedHandOffPaused = b;
    }

    public List<String> listEndpointsPendingHints()
    {
        Token.TokenFactory tokenFactory = StorageService.getPartitioner().getTokenFactory();

        // Extract the keys as strings to be reported.
        LinkedList<String> result = new LinkedList<String>();
        ReadCommand cmd = ReadCommands.allDataRead(CFMetaData.HintsCf, (int)(System.currentTimeMillis() / 1000));
        try (PartitionIterator iter = cmd.executeLocally(hintStore))
        {
            while (iter.hasNext())
            {
                try (AtomIterator partition = iter.next())
                {
                    // We don't delete by range on the hints table, so we don't have to worry about the
                    // iterator returning only range tombstone marker
                    if (partition.hasNext())
                        result.addFirst(tokenFactory.toString(partition.partitionKey().getToken()));
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }
}
