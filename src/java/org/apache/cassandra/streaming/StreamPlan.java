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
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    // global stream operation manager
    private static final StreamManager MANAGER = StreamManager.instance;
    private static final ListeningExecutorService streamExecutor = MoreExecutors.listeningDecorator(
                                                       new DebuggableThreadPoolExecutor(FBUtilities.getAvailableProcessors(),
                                                                                        Integer.MAX_VALUE,
                                                                                        TimeUnit.MILLISECONDS,
                                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                                        new NamedThreadFactory("StreamSession")));

    // plan ID will be auto generated when not given
    private UUID planId;
    private final OperationType type;
    // sessions per InetAddress of the other end.
    private final Map<InetAddress, StreamSession> sessions = new HashMap<>();

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param type Stream type that describes this StreamPlan
     */
    public StreamPlan(OperationType type)
    {
        this.type = type;
    }

    /**
     * Force building plan to have specific plan ID.
     *
     * @param planId Plan ID to set to. Must not be null.
     * @return this object for chaining
     */
    public StreamPlan planId(UUID planId)
    {
        this.planId = Preconditions.checkNotNull(planId);
        return this;
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges)
    {
        return requestRanges(from, keyspace, ranges, new String[0]);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(from);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies));
        return this;
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges)
    {
        return transferRanges(to, keyspace, ranges, new String[0]);
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer);
        return this;
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param ranges ranges to send
     * @param sstables files to send
     * @return this object for chaining
     */
    public StreamPlan transferFiles(InetAddress to, Collection<Range<Token>> ranges, Collection<SSTableReader> sstables)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferFiles(ranges, sstables);
        return this;
    }

    public StreamPlan bind(Socket socket)
    {
        if (!sessions.containsKey(socket.getInetAddress()))
        {
            StreamSession session = new StreamSession(socket);
            sessions.put(socket.getInetAddress(), session);
        }
        return this;
    }

    /**
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty()
    {
        return sessions.isEmpty();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute()
    {
        if (planId == null)
            planId = UUIDGen.getTimeUUID();
        StreamResultFuture streamResult = new StreamResultFuture(planId, type, sessions.size());

        MANAGER.register(streamResult);

        // start sessions
        for (StreamSession session : sessions.values())
        {
            session.register(streamResult);
            // register to gossiper/FD to fail on node failure
            Gossiper.instance.register(session);
            FailureDetector.instance.registerFailureDetectionEventListener(session);
            streamExecutor.submit(session);
        }
        return streamResult;
    }

    /**
     * Set flushBeforeTransfer option.
     * When it's true, will flush before streaming ranges. (Default: true)
     *
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this object for chaining
     */
    public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer)
    {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }

    private StreamSession getOrCreateSession(InetAddress peer)
    {
        StreamSession session = sessions.get(peer);
        if (session == null)
        {
            session = new StreamSession(peer);
            sessions.put(peer, session);
        }
        return session;
    }

}
