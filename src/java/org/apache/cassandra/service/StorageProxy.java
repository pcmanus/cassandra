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
package org.apache.cassandra.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang.StringUtils;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);
    static final boolean OPTIMIZE_LOCAL_REQUESTS = true; // set to false to test messagingservice path on single node

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static volatile boolean hintedHandoffEnabled = DatabaseDescriptor.hintedHandoffEnabled();
    private static volatile int maxHintWindow = DatabaseDescriptor.getMaxHintWindow();
    private static volatile int maxHintsInProgress = 1024 * FBUtilities.getAvailableProcessors();
    private static final AtomicInteger totalHintsInProgress = new AtomicInteger();
    private static final Map<InetAddress, AtomicInteger> hintsInProgress = new MapMaker().concurrencyLevel(1).makeComputingMap(new Function<InetAddress, AtomicInteger>()
    {
        public AtomicInteger apply(InetAddress inetAddress)
        {
            return new AtomicInteger(0);
        }
    });
    private static final AtomicLong totalHints = new AtomicLong();

    private static final Function<Row, List<Row>> makeSingletonListFunction = new Function<Row, List<Row>>()
    {
        public List<Row> apply(Row row)
        {
            return Collections.singletonList(row);
        }
    };

    private StorageProxy() {}

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(new StorageProxy(), new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        standardWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              WriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws OverloadedException
            {
                assert mutation instanceof RowMutation;
                sendToHintedEndpoints((RowMutation) mutation, targets, responseHandler, localDataCenter, consistency_level);
            }
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the MUTATION stage
         * but on the latter case, the verb handler already run on the MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              WriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            {
                if (logger.isTraceEnabled())
                    logger.trace("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, targets, responseHandler, localDataCenter, consistency_level);
                runnable.run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              WriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            {
                if (logger.isTraceEnabled())
                    logger.trace("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, targets, responseHandler, localDataCenter, consistency_level);
                StageManager.getStage(Stage.MUTATION).execute(runnable);
            }
        };
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the ones given by @param old.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @return true if the operation succeeds in updating the row
     */
    public static ListenableFuture<Boolean> cas(String table, String cfName, ByteBuffer key, ColumnFamily expected, ColumnFamily updates)
    throws UnavailableException, InvalidRequestException, IsBootstrappingException
    {
        // We need to read to do a CAS, and we can't do one while we bootstrap
        if (StorageService.instance.isBootstrapMode())
            throw new IsBootstrappingException();

        CFMetaData metadata = Schema.instance.getCFMetaData(table, cfName);

        TimeoutingFuture<Boolean> future = TimeoutingFuture.create(RequestType.CAS_WRITE);
        try
        {
            return future.register(doCas(metadata, key, expected, updates));
        }
        catch (UnavailableException e)
        {
            future.setUnavailable();
            throw e;
        }
        catch (InvalidRequestException e)
        {
            future.cancel(true);
            throw e;
        }
        catch (RuntimeException e)
        {
            future.cancel(true);
            throw e;
        }
    }

    private static RequestTracker<Boolean> doCas(final CFMetaData metadata,
                                                 final ByteBuffer key,
                                                 final ColumnFamily expected,
                                                 final ColumnFamily updates)
    throws UnavailableException, InvalidRequestException
    {
        final TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();

        // for simplicity, we'll do a single liveness check at the start of each attempt
        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(metadata.ksName, key);
        final List<InetAddress> liveEndpoints = p.left;
        final int requiredParticipants = p.right;

        RequestTracker<UUID> prepareResult = beginAndRepairPaxos(key, metadata, liveEndpoints, requiredParticipants);
        ListenableFuture<Boolean> future = Trackers.transform(prepareResult, reporter, onPaxosPrepareResult(metadata, key, expected, updates, liveEndpoints, requiredParticipants, reporter));
        return Trackers.of(future, reporter);
    }

    private static AsyncFunction<UUID, Boolean> onPaxosPrepareResult(final CFMetaData metadata,
                                                                     final ByteBuffer key,
                                                                     final ColumnFamily expected,
                                                                     final ColumnFamily updates,
                                                                     final List<InetAddress> liveEndpoints,
                                                                     final int requiredParticipants,
                                                                     final TimeoutReporter.Updatable reporter)
    {
        return new AsyncFunction<UUID, Boolean>()
        {
            public ListenableFuture<Boolean> apply(final UUID ballot)
            {
                try
                {
                    if (ballot == null)
                    {
                        RequestTracker<Boolean> retry = doCas(metadata, key, expected, updates);
                        reporter.switchTo(retry);
                        return retry.future();
                    }

                    // read the current value and compare with expected
                    Tracing.trace("Reading existing values for CAS precondition");
                    ReadCommand readCommand = expected == null
                                            ? new SliceFromReadCommand(metadata.ksName, key, metadata.cfName, new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1))
                                            : new SliceByNamesReadCommand(metadata.ksName, key, metadata.cfName, new NamesQueryFilter(ImmutableSortedSet.copyOf(expected.getColumnNames())));

                    RequestTracker<Row> readFuture = fetchRow(readCommand, ConsistencyLevel.QUORUM);
                    return Trackers.transform(readFuture, reporter, new AsyncFunction<Row, Boolean>()
                    {
                        public ListenableFuture<Boolean> apply(Row row)
                        {
                            ColumnFamily current = row.cf;
                            if (!casApplies(expected, current))
                            {
                                Tracing.trace("CAS precondition {} does not match current values {}", expected, current);
                                return Futures.immediateFuture(false);
                            }

                            // finish the paxos round w/ the desired updates
                            // TODO turn null updates into delete?
                            final Commit proposal = Commit.newProposal(key, ballot, updates);
                            Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);

                            RequestTracker<Boolean> proposeFuture = proposePaxos(proposal, liveEndpoints, requiredParticipants);
                            return Trackers.transform(proposeFuture, reporter, onPaxosProposeResult(metadata, key, expected, updates, liveEndpoints, proposal, reporter));
                        }
                    });
                }
                catch (Exception e)
                {
                    return Futures.immediateFailedFuture(e);
                }
            }
        };
    }

    private static AsyncFunction<Boolean, Boolean> onPaxosProposeResult(final CFMetaData metadata,
                                                                        final ByteBuffer key,
                                                                        final ColumnFamily expected,
                                                                        final ColumnFamily updates,
                                                                        final List<InetAddress> liveEndpoints,
                                                                        final Commit proposal,
                                                                        final TimeoutReporter.Updatable reporter)
    {
        return new AsyncFunction<Boolean, Boolean>()
        {
            public ListenableFuture<Boolean> apply(Boolean success)
            {
                if (success)
                {
                    commitPaxos(proposal, liveEndpoints);
                    Tracing.trace("CAS successful");
                    return Futures.immediateFuture(true);
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                Uninterruptibles.sleepUninterruptibly(FBUtilities.threadLocalRandom().nextInt(100), TimeUnit.MILLISECONDS);

                try
                {
                    RequestTracker<Boolean> retry = doCas(metadata, key, expected, updates);
                    reporter.switchTo(retry);
                    return retry.future();
                }
                catch (Exception e)
                {
                    return Futures.immediateFailedFuture(e);
                }
            }
        };
    }

    private static boolean hasLiveColumns(ColumnFamily cf)
    {
        return cf != null && !cf.hasOnlyTombstones();
    }

    private static boolean casApplies(ColumnFamily expected, ColumnFamily current)
    {
        if (!hasLiveColumns(expected))
            return !hasLiveColumns(current);
        else if (!hasLiveColumns(current))
            return false;

        // current has been built from expected, so we know that it can't have columns
        // that excepted don't have. So we just check that for each columns in expected:
        //   - if it is a tombstone, whether current has no column or a tombstone;
        //   - otherwise, that current has a live column with the same value.
        for (Column e : expected)
        {
            Column c = current.getColumn(e.name());
            if (e.isLive())
            {
                if (!(c != null && c.isLive() && c.value().equals(e.value())))
                    return false;
            }
            else
            {
                if (c != null && c.isLive())
                    return false;
            }
        }
        return true;
    }

    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(String table, ByteBuffer key) throws UnavailableException
    {
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);
        int requiredParticipants = pendingEndpoints.size() + 1 + naturalEndpoints.size() / 2; // See CASSANDRA-833
        List<InetAddress> liveEndpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), IAsyncCallback.isAlive));
        if (liveEndpoints.size() < requiredParticipants)
            throw new UnavailableException(ConsistencyLevel.SERIAL, requiredParticipants, liveEndpoints.size());
        return Pair.create(liveEndpoints, requiredParticipants);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static RequestTracker<UUID> beginAndRepairPaxos(ByteBuffer key, CFMetaData metadata, final List<InetAddress> liveEndpoints, final int requiredParticipants)
    {
        final TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();

        final UUID ballot = UUIDGen.getTimeUUID();

        // prepare
        Tracing.trace("Preparing {}", ballot);
        Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
        RequestTracker<PrepareSummary> prepareResult = preparePaxos(toPrepare, liveEndpoints, requiredParticipants);
        ListenableFuture<UUID> future = Trackers.transform(prepareResult, reporter, new AsyncFunction<PrepareSummary, UUID>()
        {
            public ListenableFuture<UUID> apply(PrepareSummary summary)
            {
                if (!summary.promised)
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    Uninterruptibles.sleepUninterruptibly(FBUtilities.threadLocalRandom().nextInt(100), TimeUnit.MILLISECONDS);
                    return Futures.immediateFuture(null);
                }

                final Commit inProgress = summary.inProgressCommit;
                final Commit mostRecent = summary.mostRecentCommit;

                // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
                // needs to be completed, so do it.
                if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
                {
                    Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                    ProposeCallback proposeFuture = proposePaxos(inProgress, liveEndpoints, requiredParticipants);
                    return Trackers.transform(proposeFuture, reporter, new Function<Boolean, UUID>()
                    {
                        public UUID apply(Boolean success)
                        {
                            if (success)
                                commitPaxos(inProgress, liveEndpoints);

                            // no need to sleep here
                            return null;
                        }
                    });
                }

                // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
                // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
                // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
                // mean we lost messages), we pro-actively "repair" those nodes, and retry.
                Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit;
                if (Iterables.size(missingMRC) > 0)
                {
                    Tracing.trace("Repairing replicas that missed the most recent commit");
                    commitPaxos(mostRecent, missingMRC);
                    // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                    // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                    // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                    // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                    return Futures.immediateFuture(null);
                }

                return Futures.immediateFuture(ballot);
            }
        });

        return Trackers.of(future, reporter);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants)
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.key, toPrepare.update.metadata(), requiredParticipants);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        return callback;
    }

    private static ProposeCallback proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants)
    {
        ProposeCallback callback = new ProposeCallback(requiredParticipants);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        return callback;
    }

    private static void commitPaxos(Commit proposal, Iterable<InetAddress> endpoints)
    {
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendOneWay(message, target);
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     * @return a future on the result of the write.
     */
    public static ListenableFuture<List<Void>> mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException
    {
        Tracing.trace("Determining replicas for mutation");
        logger.trace("Mutations/ConsistencyLevel are {}/{}", mutations, consistency_level);
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        TimeoutingFuture<List<Void>> future = TimeoutingFuture.create(RequestType.WRITE);
        try
        {
            List<RequestTracker<Void>> trackers = new ArrayList<RequestTracker<Void>>();
            for (IMutation mutation : mutations)
            {
                if (mutation instanceof CounterMutation)
                {
                    trackers.add(mutateCounter((CounterMutation)mutation, localDataCenter));
                }
                else
                {
                    WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;
                    trackers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer, wt));
                }
            }
            return future.register(Trackers.combine(trackers));
        }
        catch (UnavailableException e)
        {
            future.cancel(true);
            RequestType.WRITE.reportUnavailable();
            throw e;
        }
        catch (OverloadedException e)
        {
            future.cancel(true);
            RequestType.WRITE.reportOverloaded();
            throw e;
        }
        catch (RuntimeException e)
        {
            future.cancel(true);
            throw e;
        }
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param rowMutations the RowMutations to be applied across the replicas
     * @param consistencyLevel the consistency level for the operation
     */
    public static ListenableFuture<List<Void>> mutateAtomically(Collection<RowMutation> rowMutations, final ConsistencyLevel consistencyLevel)
    throws UnavailableException, OverloadedException
    {
        Tracing.trace("Determining replicas for atomic batch");
        logger.trace("Mutations/ConsistencyLevel are {}/{}", rowMutations, consistencyLevel);
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        final TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();

        // Copy in a list to ensure iteration order
        final List<RowMutation> mutations = new ArrayList<RowMutation>(rowMutations);
        final List<WriteResponseHandler> handlers = new ArrayList<WriteResponseHandler>(mutations.size());

        TimeoutingFuture<List<Void>> future = TimeoutingFuture.create(RequestType.WRITE);
        try
        {
            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (RowMutation mutation : mutations)
            {
                WriteResponseHandler handler = getWriteResponseHandler(mutation, consistencyLevel, WriteType.BATCH);
                // exit early if we can't fulfill the CL at this time.
                handler.assureSufficientLiveNodes();
                handlers.add(handler);
            }

            // write to the batchlog
            final Collection<InetAddress> batchlogEndpoints = getBatchlogEndpoints(localDataCenter);
            final UUID batchUUID = UUID.randomUUID();
            RequestTracker<Void> batchlogWrite = writeToBatchlog(mutations, batchlogEndpoints, batchUUID);

            ListenableFuture<List<Void>> writeFuture = Trackers.transform(batchlogWrite, reporter, new AsyncFunction<Void, List<Void>>()
            {
                public ListenableFuture<List<Void>> apply(Void v)
                {
                    RequestTracker<List<Void>> tracker = Trackers.combine(handlers);
                    reporter.switchTo(tracker);

                    try
                    {
                        // now actually send the writes and wait for them to complete
                        for (int i = 0; i < mutations.size(); i++)
                        {
                            WriteResponseHandler handler = handlers.get(i);
                            sendToHintedEndpoints(mutations.get(i), handler.allEndpoints(), handler, localDataCenter, consistencyLevel);
                        }
                        return tracker.future();
                    }
                    catch (Exception e)
                    {
                        return Futures.immediateFailedFuture(e);
                    }
                }
            });
            writeFuture.addListener(new Runnable()
            {
                public void run()
                {
                    // remove the batchlog entries asynchronously
                    asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID);
                }
            }, MoreExecutors.sameThreadExecutor());
            return future.register(Trackers.of(writeFuture, reporter));
        }
        catch (UnavailableException e)
        {
            future.cancel(true);
            RequestType.WRITE.reportUnavailable();
            throw e;
        }
        catch (RuntimeException e)
        {
            future.cancel(true);
            throw e;
        }
    }

    private static RequestTracker<Void> writeToBatchlog(Collection<RowMutation> mutations, Collection<InetAddress> endpoints, UUID uuid)
    {
        RowMutation rm = BatchlogManager.getBatchlogMutationFor(mutations, uuid);
        WriteResponseHandler handler = new WriteResponseHandler(Table.open(Table.SYSTEM_KS),
                                                                endpoints,
                                                                Collections.<InetAddress>emptyList(),
                                                                ConsistencyLevel.ONE,
                                                                WriteType.BATCH_LOG);
        updateBatchlog(rm, endpoints, handler);
        return handler;
    }

    private static void asyncRemoveFromBatchlog(Collection<InetAddress> endpoints, UUID uuid)
    {
        ColumnFamily cf = EmptyColumns.factory.create(Schema.instance.getCFMetaData(Table.SYSTEM_KS, SystemTable.BATCHLOG_CF));
        cf.delete(new DeletionInfo(FBUtilities.timestampMicros(), (int) (System.currentTimeMillis() / 1000)));
        WriteResponseHandler handler = new WriteResponseHandler(Table.open(Table.SYSTEM_KS),
                                                                endpoints,
                                                                Collections.<InetAddress>emptyList(),
                                                                ConsistencyLevel.ANY,
                                                                WriteType.SIMPLE);
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, UUIDType.instance.decompose(uuid), cf);
        updateBatchlog(rm, endpoints, handler);
    }

    private static void updateBatchlog(RowMutation rm, Collection<InetAddress> endpoints, WriteResponseHandler handler)
    {
        if (endpoints.contains(FBUtilities.getBroadcastAddress()))
        {
            assert endpoints.size() == 1;
            insertLocal(rm, handler);
        }
        else
        {
            sendMessagesToOneDC(rm.createMessage(), endpoints, true, handler);
        }
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistency_level the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     */
    public static WriteResponseHandler performWrite(IMutation mutation,
                                                    ConsistencyLevel consistencyLevel,
                                                    String localDataCenter,
                                                    WritePerformer performer,
                                                    WriteType writeType)
    throws UnavailableException, OverloadedException
    {
        WriteResponseHandler responseHandler = getWriteResponseHandler(mutation, consistencyLevel, writeType);
        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, responseHandler.allEndpoints(), responseHandler, localDataCenter, consistencyLevel);
        return responseHandler;
    }

    private static WriteResponseHandler getWriteResponseHandler(IMutation mutation, ConsistencyLevel consistencyLevel, WriteType writeType)
    {
        String table = mutation.getTable();
        AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();

        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

        return rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, writeType);
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node cluster
     */
    private static Collection<InetAddress> getBatchlogEndpoints(String localDataCenter) throws UnavailableException
    {
        // will include every known node in the DC, including localhost.
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap().getTopology();
        Collection<InetAddress> localMembers = topology.getDatacenterEndpoints().get(localDataCenter);

        // special case for single-node datacenters
        if (localMembers.size() == 1)
            return localMembers;

        // not a single-node cluster - don't count the local node.
        localMembers.remove(FBUtilities.getBroadcastAddress());

        // include only alive nodes
        List<InetAddress> candidates = new ArrayList<InetAddress>(localMembers.size());
        for (InetAddress member : localMembers)
        {
            if (FailureDetector.instance.isAlive(member))
                candidates.add(member);
        }

        if (candidates.isEmpty())
            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);

        if (candidates.size() > 2)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), candidates);
            candidates = candidates.subList(0, 2);
        }

        return candidates;
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     *
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     */
    public static void sendToHintedEndpoints(final RowMutation rm,
                                             Iterable<InetAddress> targets,
                                             WriteResponseHandler responseHandler,
                                             String localDataCenter,
                                             ConsistencyLevel consistency_level)
    throws OverloadedException
    {
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<MessageOut, InetAddress>> dcMessages = null;

        for (InetAddress destination : targets)
        {
            // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
            // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
            // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
            // a small number of nodes causing problems, so we should avoid shutting down writes completely to
            // healthy nodes.  Any node with no hintsInProgress is considered healthy.
            if (totalHintsInProgress.get() > maxHintsInProgress
                && (hintsInProgress.get(destination).get() > 0 && shouldHint(destination)))
            {
                throw new OverloadedException("Too many in flight hints: " + totalHintsInProgress.get());
            }

            if (FailureDetector.instance.isAlive(destination))
            {
                if (destination.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                {
                    insertLocal(rm, responseHandler);
                }
                else
                {
                    // belongs on a different server
                    if (logger.isTraceEnabled())
                        logger.trace("insert writing key " + ByteBufferUtil.bytesToHex(rm.key()) + " to " + destination);

                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                    Multimap<MessageOut, InetAddress> messages = (dcMessages != null) ? dcMessages.get(dc) : null;
                    if (messages == null)
                    {
                        messages = HashMultimap.create();
                        if (dcMessages == null)
                            dcMessages = new HashMap<String, Multimap<MessageOut, InetAddress>>();
                        dcMessages.put(dc, messages);
                    }

                    messages.put(rm.createMessage(), destination);
                }
            }
            else
            {
                if (!shouldHint(destination))
                    continue;

                // Schedule a local hint
                submitHint(rm, destination, responseHandler, consistency_level);
            }
        }

        if (dcMessages != null)
            sendMessages(localDataCenter, dcMessages, responseHandler);
    }

    public static Future<Void> submitHint(final RowMutation mutation,
                                          final InetAddress target,
                                          final WriteResponseHandler responseHandler,
                                          final ConsistencyLevel consistencyLevel)
    {
        // local write that time out should be handled by LocalMutationRunnable
        assert !target.equals(FBUtilities.getBroadcastAddress()) : target;

        HintRunnable runnable = new HintRunnable(target)
        {
            public void runMayThrow()
            {
                logger.debug("Adding hint for {}", target);

                writeHintForMutation(mutation, target);
                // Notify the handler only for CL == ANY
                if (responseHandler != null && consistencyLevel == ConsistencyLevel.ANY)
                    responseHandler.response(null);
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        totalHintsInProgress.incrementAndGet();
        hintsInProgress.get(runnable.target).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    public static void writeHintForMutation(RowMutation mutation, InetAddress target)
    {
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(target);
        assert hostId != null : "Missing host ID for " + target.getHostAddress();
        RowMutation hintedMutation = HintedHandOffManager.hintFor(mutation, hostId);
        hintedMutation.apply();

        totalHints.incrementAndGet();
    }

    /**
     * for each datacenter, send a message to one node to relay the write to other replicas
     */
    private static void sendMessages(String localDataCenter, Map<String, Multimap<MessageOut, InetAddress>> dcMessages, WriteResponseHandler handler)
    {
        for (Map.Entry<String, Multimap<MessageOut, InetAddress>> entry: dcMessages.entrySet())
        {
            boolean isLocalDC = entry.getKey().equals(localDataCenter);
            for (Map.Entry<MessageOut, Collection<InetAddress>> messages: entry.getValue().asMap().entrySet())
            {
                MessageOut message = messages.getKey();
                Collection<InetAddress> targets = messages.getValue();
                // a single message object is used for unhinted writes, so clean out any forwards
                // from previous loop iterations
                message = message.withHeaderRemoved(RowMutation.FORWARD_TO);
                sendMessagesToOneDC(message, targets, isLocalDC, handler);
            }
        }
    }

    private static void sendMessagesToOneDC(MessageOut message, Collection<InetAddress> targets, boolean localDC, WriteResponseHandler handler)
    {
        Iterator<InetAddress> iter = targets.iterator();
        InetAddress target = iter.next();

        // direct writes to local DC or old Cassandra versions
        // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
        if (localDC || MessagingService.instance().getVersion(target) < MessagingService.VERSION_20)
        {
            // yes, the loop and non-loop code here are the same; this is clunky but we want to avoid
            // creating a second iterator since we already have a perfectly good one
            MessagingService.instance().sendRR(message, target, handler);
            while (iter.hasNext())
            {
                target = iter.next();
                MessagingService.instance().sendRR(message, target, handler);
            }
            return;
        }

        // Add all the other destinations of the same message as a FORWARD_HEADER entry
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            out.writeInt(targets.size() - 1);
            while (iter.hasNext())
            {
                InetAddress destination = iter.next();
                CompactEndpointSerializationHelper.serialize(destination, out);
                int id = MessagingService.instance().addCallback(handler, message, destination, message.getTimeout());
                out.writeInt(id);
                logger.trace("Adding FWD message to {}@{}", id, destination);
            }
            message = message.withParameter(RowMutation.FORWARD_TO, out.getData());
            // send the combined message + forward headers
            int id = MessagingService.instance().sendRR(message, target, handler);
            logger.trace("Sending message to {}@{}", id, target);
        }
        catch (IOException e)
        {
            // DataOutputBuffer is in-memory, doesn't throw IOException
            throw new AssertionError(e);
        }
    }

    private static void insertLocal(final RowMutation rm, final WriteResponseHandler responseHandler)
    {
        if (logger.isTraceEnabled())
            logger.trace("insert writing local " + rm.toString(true));

        Runnable runnable = new DroppableRunnable(MessagingService.Verb.MUTATION)
        {
            public void runMayThrow()
            {
                rm.apply();
                responseHandler.response(null);
            }
        };
        StageManager.getStage(Stage.MUTATION).execute(runnable);
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static WriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, OverloadedException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getTable(), cm.key(), localDataCenter, cm.consistency());

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String table = cm.getTable();
            AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();
            Token tk = StorageService.getPartitioner().getToken(cm.key());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

            rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, cm.consistency(), WriteType.COUNTER).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            WriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.COUNTER);

            if (logger.isTraceEnabled())
                logger.trace("forwarding counter update of key " + ByteBufferUtil.bytesToHex(cm.key()) + " to " + endpoint);
            MessagingService.instance().sendRR(cm.makeMutationMessage(), endpoint, responseHandler);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(String tableName, ByteBuffer key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Table table = Table.open(tableName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(table, key);
        if (endpoints.isEmpty())
            // TODO have a way to compute the consistency level
            throw new UnavailableException(cl, cl.blockFor(table), 0);

        List<InetAddress> localEndpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : endpoints)
        {
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);
        }
        if (localEndpoints.isEmpty())
        {
            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(FBUtilities.threadLocalRandom().nextInt(localEndpoints.size()));
        }
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static WriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, WriteType.COUNTER);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static WriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, WriteType.COUNTER);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Iterable<InetAddress> targets,
                                             final WriteResponseHandler responseHandler,
                                             final String localDataCenter,
                                             final ConsistencyLevel consistency_level)
    {
        return new LocalMutationRunnable()
        {
            public void runMayThrow()
            {
                assert mutation instanceof CounterMutation;
                final CounterMutation cm = (CounterMutation) mutation;

                // apply mutation
                cm.apply();
                responseHandler.response(null);

                // then send to replicas, if any
                final Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets), ImmutableSet.of(FBUtilities.getBroadcastAddress()));
                if (cm.shouldReplicateOnWrite() && !remotes.isEmpty())
                {
                    // We do the replication on another stage because it involves a read (see CM.makeReplicationMutation)
                    // and we want to avoid blocking too much the MUTATION stage
                    StageManager.getStage(Stage.REPLICATE_ON_WRITE).execute(new DroppableRunnable(MessagingService.Verb.READ)
                    {
                        public void runMayThrow() throws OverloadedException
                        {
                            // send mutation to other replica
                            sendToHintedEndpoints(cm.makeReplicationMutation(), remotes, responseHandler, localDataCenter, consistency_level);
                        }
                    });
                }
            }
        };
    }

    private static boolean systemTableQuery(List<ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!cmd.table.equals(Table.SYSTEM_KS))
                return false;
        return true;
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static ListenableFuture<List<Row>> read(List<ReadCommand> commands, ConsistencyLevel consistencyLevel)
    throws UnavailableException, IsBootstrappingException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode() && !systemTableQuery(commands))
        {
            RequestType.READ.reportUnavailable();
            throw new IsBootstrappingException();
        }

        boolean isCas = consistencyLevel == ConsistencyLevel.SERIAL;
        TimeoutingFuture<List<Row>> future = TimeoutingFuture.create(isCas ? RequestType.CAS_READ : RequestType.READ);
        try
        {
            if (isCas)
            {
                // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
                if (commands.size() > 1)
                    throw new InvalidRequestException("SERIAL consistency may only be requested for one row at a time");

                final ReadCommand command = commands.get(0);
                CFMetaData metadata = Schema.instance.getCFMetaData(command.table, command.cfName);

                final TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();
                RequestTracker<UUID> paxosResult = preparePaxosForSerialRead(metadata, command.key);
                ListenableFuture<Row> readFuture = Trackers.transform(paxosResult, reporter, new AsyncFunction<UUID, Row>()
                {
                    public ListenableFuture<Row> apply(UUID ballot)
                    {
                        try
                        {
                            RequestTracker<Row> tracker = fetchRow(command, ConsistencyLevel.QUORUM);
                            reporter.switchTo(tracker);
                            return tracker.future();
                        }
                        catch (UnavailableException e)
                        {
                            return Futures.immediateFailedFuture(e);
                        }
                    }
                });

                future.register(Trackers.of(Futures.transform(readFuture, makeSingletonListFunction), reporter));
            }
            else
            {
                List<RequestTracker<Row>> trackers = new ArrayList<RequestTracker<Row>>(commands.size());
                for (ReadCommand command : commands)
                    trackers.add(fetchRow(command, consistencyLevel));
                future.register(Trackers.combine(trackers));
            }
        }
        catch (UnavailableException e)
        {
            future.setUnavailable();
            throw e;
        }
        catch (InvalidRequestException e)
        {
            future.cancel(true);
            throw e;
        }
        catch (RuntimeException e)
        {
            future.setException(e);
        }
        return future;
    }

    private static RequestTracker<UUID> preparePaxosForSerialRead(final CFMetaData metadata, final ByteBuffer rowKey)
    throws UnavailableException
    {
        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(metadata.ksName, rowKey);
        List<InetAddress> liveEndpoints = p.left;
        int requiredParticipants = p.right;

        // We ignore the timeout provider on purpose, this dealt with externally
        final ListenableFuture<UUID> future = beginAndRepairPaxos(rowKey, metadata, liveEndpoints, requiredParticipants).future();
        return new RequestTracker<UUID>()
        {
            public ListenableFuture<UUID> future()
            {
                return Futures.transform(future, new AsyncFunction<UUID, UUID>()
                {
                    public ListenableFuture<UUID> apply(UUID ballot)
                    {
                        if (ballot != null)
                            return Futures.immediateFuture(ballot);

                        try
                        {
                            // same player, try again (it's ok to ignore the timeout reporting)
                            return preparePaxosForSerialRead(metadata, rowKey).future();
                        }
                        catch (UnavailableException e)
                        {
                            return Futures.immediateFailedFuture(e);
                        }
                    }
                });
            }

            public RequestTimeoutException reportTimeout()
            {
                return new ReadTimeoutException(ConsistencyLevel.SERIAL, -1, -1, false);
            }
        };
    }

    /**
     * This function executes a local or remote read, returning a future on the result.
     *
     * It goes through the following steps:
     *   1. Get the replica locations, sorted by response time according to the snitch
     *   2. Send a data request to the closest replica, and digest requests to either
     *      a) all the replicas, if read repair is enabled
     *      b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     *   3. Wait for a response from R replicas
     *   4. If the digests (if any) match the data return the data
     *   5. else carry out read repair by getting data from all the nodes.
     */
    private static RequestTracker<Row> fetchRow(final ReadCommand command, final ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        final TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();
        final ReadCallback<ReadResponse, Row> handler = ReadExecutor.executeAsync(command, consistencyLevel);

        // Trim the row on result
        ListenableFuture<Row> future = Trackers.transform(handler, reporter, new Function<Row, Row>()
        {
            public Row apply(Row row)
            {
                command.maybeTrim(row);
                return row;
            }
        });

        FutureFallback<Row> handleMismatch = new FutureFallback<Row>()
        {
            public ListenableFuture<Row> create(Throwable t)
            {
                // We propagate anything other than a digest mismatch
                if (!(t instanceof DigestMismatchException))
                    return Futures.<Row>immediateFailedFuture(t);

                logger.trace("Digest mismatch: {}", t);

                // Do a full data read to resolve the correct response (and repair node that need be)
                final RowDataResolver repairResolver = new RowDataResolver(command.table, command.key, command.filter());
                final ReadCallback<ReadResponse, Row> repairHandler = handler.withNewResolver(repairResolver);

                // Send the repair messages
                MessageOut<ReadCommand> message = command.createMessage();
                for (InetAddress endpoint : handler.endpoints)
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);

                return Trackers.transform(repairHandler, reporter, new AsyncFunction<Row, Row>()
                {
                    public ListenableFuture<Row> apply(Row row)
                    {
                        // Wait for repair futures
                        reporter.switchTo(new TimeoutReporter()
                        {
                            public RequestTimeoutException reportTimeout()
                            {
                                int blockFor = consistencyLevel.blockFor(Table.open(command.getKeyspace()));
                                return new ReadTimeoutException(consistencyLevel, blockFor, blockFor, true);
                            }
                        });

                        return Futures.transform(repairResolver.repairFuture, withShortReadRetry(command, consistencyLevel, row, repairResolver, reporter));
                    }
                });
            }
        };

        return Trackers.of(Futures.withFallback(future, handleMismatch), reporter);
    }

    private static AsyncFunction<Object, Row> withShortReadRetry(final ReadCommand command,
                                                                 final ConsistencyLevel consistencyLevel,
                                                                 final Row row,
                                                                 final RowDataResolver repairResolver,
                                                                 final TimeoutReporter.Updatable reporter)
    {
        return new AsyncFunction<Object, Row>()
        {
            public ListenableFuture<Row> apply(Object o)
            {
                try
                {
                    // Was that a short reads
                    ReadCommand retryCommand = command.maybeGenerateRetryCommand(repairResolver, row);
                    if (retryCommand != null)
                    {
                        logger.debug("Issuing retry for read command");
                        RequestTracker<Row> retry = fetchRow(retryCommand, consistencyLevel);
                        reporter.switchTo(retry);
                        return retry.future();
                    }

                    if (row != null)
                        command.maybeTrim(row);
                    return Futures.immediateFuture(row);
                }
                catch (UnavailableException e)
                {
                    return Futures.immediateFailedFuture(e);
                }
            }
        };
    }

    static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback<ReadResponse, Row> handler;
        private final long start = System.currentTimeMillis();

        LocalReadRunnable(ReadCommand command, ReadCallback<ReadResponse, Row> handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            logger.trace("LocalReadRunnable reading {}", command);

            Table table = Table.open(command.table);
            Row r = command.getRow(table);
            ReadResponse result = ReadVerbHandler.getResponse(command, r);
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    static class LocalRangeSliceRunnable extends DroppableRunnable
    {
        private final RangeSliceCommand command;
        private final ReadCallback<RangeSliceReply, Iterable<Row>> handler;
        private final long start = System.currentTimeMillis();

        LocalRangeSliceRunnable(RangeSliceCommand command, ReadCallback<RangeSliceReply, Iterable<Row>> handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            logger.trace("LocalReadRunnable reading {}", command);

            RangeSliceReply result = new RangeSliceReply(RangeSliceVerbHandler.executeLocally(command));
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    public static List<InetAddress> getLiveSortedEndpoints(Table table, ByteBuffer key)
    {
        return getLiveSortedEndpoints(table, StorageService.instance.getPartitioner().decorateKey(key));
    }

    private static List<InetAddress> getLiveSortedEndpoints(Table table, RingPosition pos)
    {
        List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(table, pos);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
        return liveEndpoints;
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2)
    {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

    public static ListenableFuture<List<Row>> getRangeSlice(RangeSliceCommand command, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        Tracing.trace("Determining replicas to query");
        logger.trace("Command/ConsistencyLevel is {}/{}", command.toString(), consistencyLevel);

        TimeoutingFuture<List<Row>> future = TimeoutingFuture.create(RequestType.RANGE);

        Table table = Table.open(command.keyspace);
        // now scan until we have enough results
        try
        {
            PeekingIterator<AbstractBounds<RowPosition>> ranges = Iterators.peekingIterator(getRestrictedRanges(command.range).iterator());
            IDiskAtomFilter commandPredicate = command.predicate;
            TimeoutReporter.Updatable reporter = new TimeoutReporter.Updatable();
            ListenableFuture<List<Row>> readFuture = getNextRestrictedRangeSlice(table,
                                                                                 command,
                                                                                 command.predicate,
                                                                                 ranges,
                                                                                 consistencyLevel,
                                                                                 new ArrayList<Row>(),
                                                                                 0,
                                                                                 reporter);
            return future.register(Trackers.of(Futures.transform(readFuture, trimFunction(command)), reporter));
        }
        catch (UnavailableException e)
        {
            future.setUnavailable();
            throw e;
        }
    }

    private static ListenableFuture<List<Row>> getNextRestrictedRangeSlice(final Table table,
                                                                           final RangeSliceCommand command,
                                                                           final IDiskAtomFilter commandPredicate,
                                                                           final PeekingIterator<AbstractBounds<RowPosition>> ranges,
                                                                           final ConsistencyLevel consistencyLevel,
                                                                           final List<Row> currentRows,
                                                                           final int count,
                                                                           final TimeoutReporter.Updatable reporter) throws UnavailableException
    {
        if (!ranges.hasNext())
            return Futures.immediateFuture(currentRows);

        AbstractBounds<RowPosition> range = ranges.next();
        List<InetAddress> liveEndpoints = getLiveSortedEndpoints(table, range.right);
        List<InetAddress> filteredEndpoints = consistencyLevel.filterForQuery(table, liveEndpoints);

        /*
         * getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
         * the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
         * still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
         *
         * If the current range right is the min token, we should also stop merging because CFS.getRangeSlice
         * don't know how to deal with a wrapping range.
         * Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
         * the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
         * wire compatibility, so it's likely easier not to bother.
         */

        while (ranges.hasNext() && !range.right.isMinimum())
        {
            AbstractBounds<RowPosition> nextRange = ranges.peek();
            List<InetAddress> nextEndpoints = getLiveSortedEndpoints(table, nextRange.right);

            List<InetAddress> merged = intersection(liveEndpoints, nextEndpoints);

            // Check if there is enough endpoint for the merge to be possible.
            if (!consistencyLevel.isSufficientLiveNodes(table, merged))
                break;

            List<InetAddress> nextFilteredEndpoints = consistencyLevel.filterForQuery(table, nextEndpoints);
            List<InetAddress> filteredMerged = consistencyLevel.filterForQuery(table, merged);

            // Estimate whether merging will be a win or not
            if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, filteredEndpoints, nextFilteredEndpoints))
                break;

            // If we get there, merge this range and the next one
            range = range.withNewRight(nextRange.right);
            liveEndpoints = merged;
            filteredEndpoints = filteredMerged;
            ranges.next(); // We peeked it already so just discard it
        }

        RangeSliceCommand nodeCmd = new RangeSliceCommand(command.keyspace,
                                                          command.column_family,
                                                          commandPredicate,
                                                          range,
                                                          command.row_filter,
                                                          command.maxResults,
                                                          command.countCQL3Rows,
                                                          command.isPaging);

        RequestTracker<Iterable<Row>> nodeFuture = getRestrictedRangeSlice(nodeCmd, filteredEndpoints, consistencyLevel);
        return Trackers.transform(nodeFuture, reporter, new AsyncFunction<Iterable<Row>, List<Row>>()
        {
            public ListenableFuture<List<Row>> apply(Iterable<Row> newRows)
            {
                int newCount = count;
                for (Row row : newRows)
                {
                    currentRows.add(row);
                    if (command.countCQL3Rows)
                        newCount += row.getLiveCount(commandPredicate);
                    logger.trace("range slices read {}", row.key);
                }

                // if we're done, great, otherwise, move to the next range
                if (!command.countCQL3Rows)
                    newCount = currentRows.size();
                if (count >= command.maxResults)
                    return Futures.immediateFuture(currentRows);

                // if we are paging and already got some rows, reset the column filter predicate,
                // so we start iterating the next row from the first column
                IDiskAtomFilter newCommandPredicate = commandPredicate;
                if (!currentRows.isEmpty() && command.isPaging)
                {
                    // We only allow paging with a slice filter (doesn't make sense otherwise anyway)
                    assert commandPredicate instanceof SliceQueryFilter;
                    newCommandPredicate = ((SliceQueryFilter)commandPredicate).withUpdatedSlices(ColumnSlice.ALL_COLUMNS_ARRAY);
                }

                try
                {
                    return getNextRestrictedRangeSlice(table, command, newCommandPredicate, ranges, consistencyLevel, currentRows, newCount, reporter);
                }
                catch (UnavailableException e)
                {
                    return Futures.immediateFailedFuture(e);
                }
            }
        });
    }


    private static RequestTracker<Iterable<Row>> getRestrictedRangeSlice(RangeSliceCommand command,
                                                                         List<InetAddress> endpoints,
                                                                         ConsistencyLevel consistencyLevel) throws UnavailableException
    {
        // collect replies and resolve according to consistency level
        final RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, endpoints);
        ReadCallback<RangeSliceReply, Iterable<Row>> handler = new ReadCallback(resolver, consistencyLevel, command, endpoints);
        handler.assureSufficientLiveNodes();
        if (endpoints.size() == 1
            && endpoints.get(0).equals(FBUtilities.getBroadcastAddress())
            && OPTIMIZE_LOCAL_REQUESTS)
        {
            logger.trace("reading data locally");
            StageManager.getStage(Stage.READ).execute(new LocalRangeSliceRunnable(command, handler));
        }
        else
        {
            MessageOut<RangeSliceCommand> message = command.createMessage();
            for (InetAddress endpoint : endpoints)
            {
                MessagingService.instance().sendRR(message, endpoint, handler);
                logger.trace("reading {} from {}", command, endpoint);
            }
        }

        // Wait for repairs on completion
        ListenableFuture<Iterable<Row>> future = Futures.transform(handler.future(), new AsyncFunction<Iterable<Row>, Iterable<Row>>()
        {
            public ListenableFuture<Iterable<Row>> apply(final Iterable<Row> rows)
            {
                return Futures.transform(resolver.repairFuture(), new Function<Object, Iterable<Row>>()
                {
                    public Iterable<Row> apply(Object o)
                    {
                        return rows;
                    }
                });
            }
        });
        return Trackers.of(future, handler);
    }

    private static Function<List<Row>, List<Row>> trimFunction(final RangeSliceCommand command)
    {
        // When countCQL3Rows, we let the caller trim the result.
        if (command.countCQL3Rows)
            return Functions.identity();

        return new Function<List<Row>, List<Row>>()
        {
            public List<Row> apply(List<Row> rows)
            {
                return rows.size() > command.maxResults ? rows.subList(0, command.maxResults) : rows;
            }
        };
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback<UUID> cb = new IAsyncCallback<UUID>()
        {
            public void response(MessageIn<UUID> message)
            {
                // record the response from the remote node.
                logger.trace("Received schema check response from {}", message.from.getHostAddress());
                versions.put(message.from, message.payload);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        MessageOut message = new MessageOut(MessagingService.Verb.SCHEMA_CHECK);
        for (InetAddress endpoint : liveHosts)
            MessagingService.instance().sendRR(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        logger.trace("My version is {}", myVersion);

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddress host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress());
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum(StorageService.getPartitioner()))
        {
            logger.trace("restricted single token match for query {}", queryRange);
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<AbstractBounds<T>>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);
        if (logger.isDebugEnabled())
            logger.trace("restricted ranges for query {} are {}", queryRange, ranges);

        return ranges;
    }

    public long getReadOperations()
    {
        return RequestType.READ.metrics.latency.count();
    }

    public long getTotalReadLatencyMicros()
    {
        return RequestType.READ.metrics.totalLatency.count();
    }

    public double getRecentReadLatencyMicros()
    {
        return RequestType.READ.metrics.getRecentLatency();
    }

    public long[] getTotalReadLatencyHistogramMicros()
    {
        return RequestType.READ.metrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return RequestType.READ.metrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getRangeOperations()
    {
        return RequestType.RANGE.metrics.latency.count();
    }

    public long getTotalRangeLatencyMicros()
    {
        return RequestType.RANGE.metrics.totalLatency.count();
    }

    public double getRecentRangeLatencyMicros()
    {
        return RequestType.RANGE.metrics.getRecentLatency();
    }

    public long[] getTotalRangeLatencyHistogramMicros()
    {
        return RequestType.RANGE.metrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentRangeLatencyHistogramMicros()
    {
        return RequestType.RANGE.metrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getWriteOperations()
    {
        return RequestType.WRITE.metrics.latency.count();
    }

    public long getTotalWriteLatencyMicros()
    {
        return RequestType.WRITE.metrics.totalLatency.count();
    }

    public double getRecentWriteLatencyMicros()
    {
        return RequestType.WRITE.metrics.getRecentLatency();
    }

    public long[] getTotalWriteLatencyHistogramMicros()
    {
        return RequestType.WRITE.metrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return RequestType.WRITE.metrics.recentLatencyHistogram.getBuckets(true);
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        DatabaseDescriptor.setHintedHandoffEnabled(b);
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(InetAddress ep)
    {
        if (!DatabaseDescriptor.hintedHandoffEnabled())
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            return false;
        }

        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            logger.trace("not hinting {} which has been down {}ms", ep, Gossiper.instance.getEndpointDowntime(ep));
        }
        return !hintWindowExpired;
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws RequestExecutionException If some of the hosts in the ring are down.
     * @throws IOException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws RequestExecutionException
    {
        try
        {
            truncate(keyspace, cfname).get();
        }
        catch (ExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), RequestExecutionException.class);
            throw Throwables.propagate(e.getCause());
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public static ListenableFuture<Void> truncate(String keyspace, String cfname)
    throws UnavailableException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF ", keyspace, cfname);
        if (isAnyHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddress> allEndpoints = Gossiper.instance.getLiveMembers();
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);
        TimeoutingFuture<Void> future = TimeoutingFuture.create(RequestType.TRUNCATE);
        try
        {
            // Send out the truncate calls and track the responses with the callbacks.
            logger.trace("Starting to send truncate messages to hosts {}", allEndpoints);
            final Truncation truncation = new Truncation(keyspace, cfname);
            MessageOut<Truncation> message = truncation.createMessage();
            for (InetAddress endpoint : allEndpoints)
                MessagingService.instance().sendRR(message, endpoint, responseHandler);

            logger.trace("Sent all truncate messages, now waiting for {} responses", blockFor);
            return future.register(responseHandler);
        }
        catch (RuntimeException e)
        {
            future.cancel(true);
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyHostDown()
    {
        return !Gossiper.instance.getUnreachableMembers().isEmpty();
    }

    public interface WritePerformer
    {
        public void apply(IMutation mutation, Iterable<InetAddress> targets, WriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws OverloadedException;
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();
        private final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(verb))
            {
                MessagingService.instance().incrementDroppedMessages(verb);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(MessagingService.Verb.MUTATION))
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.MUTATION);
                HintRunnable runnable = new HintRunnable(FBUtilities.getBroadcastAddress())
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final InetAddress target;

        protected HintRunnable(InetAddress target)
        {
            this.target = target;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                totalHintsInProgress.decrementAndGet();
                hintsInProgress.get(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return totalHints.get();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return totalHintsInProgress.get();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCasRpcTimeout() { return DatabaseDescriptor.getCasRpcTimeout(); }
    public void setCasRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasRpcTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }

}
