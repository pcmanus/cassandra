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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamSession is the center of Cassandra Streaming API.
 *
 * StreamSession on the both endpoints exchange messages and files until complete.
 *
 * It is created through {@link StreamPlan} on the initiator node,
 * and also is created directly from connected socket on the other end when received init message.
 *
 * <p>
 * StreamSession goes through several stages:
 * <ol>
 *  <li>
 *    Init
 *    <p>StreamSession in one end send init message to the other end.</p>
 *  </li>
 *  <li>
 *    Prepare
 *    <p>StreamSession in both endpoints are created, so in this phase, they exchange
 *    request and summary messages to prepare receiving/streaming files in next phase.</p>
 *  </li>
 *  <li>
 *    Stream
 *    <p>StreamSessions in both ends stream and receive files.</p>
 *  </li>
 *  <li>
 *    Complete
 *    <p>Session completes if both endpoints completed by exchanging complete message.</p>
 *  </li>
 * </ol>
 */
public class StreamSession implements Runnable, IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);

    public final InetAddress peer;
    public final RateLimiter limiter;

    // should not be null when session is started
    private StreamResultFuture streamResult;

    // stream requests to send to the peer
    private final List<StreamRequest> requests = new ArrayList<>();
    // streaming tasks are created and managed per ColumnFamily ID
    private final Map<UUID, StreamTransferTask> transfers = new HashMap<>();
    // data receivers, filled after receiving prepare message
    private final Map<UUID, StreamReceiveTask> receivers = new HashMap<>();

    public final ConnectionHandler handler;

    private int retries;

    // TODO state management
    public static enum State
    {
        INIT, PREPARING, STREAMING, COMPLETED,
    }

    private State state = State.INIT;

    private volatile boolean completed = false;
    private volatile boolean peerCompleted = false;

    /**
     * Create new streaming session with the peer.
     *
     * @param peer Address of streaming peer
     */
    public StreamSession(InetAddress peer)
    {
        this.peer = peer;
        this.handler = new ConnectionHandler(this);
        this.limiter = StreamManager.getRateLimiter(peer);
    }

    /**
     * Create streaming session from established connection.
     *
     * @param socket established connection
     */
    public StreamSession(Socket socket)
    {
        this.peer = socket.getInetAddress();
        this.handler = new ConnectionHandler(this, socket);
        this.limiter = StreamManager.getRateLimiter(peer);
    }

    public UUID planId()
    {
        return streamResult == null ? null : streamResult.planId;
    }

    /**
     * Bind this session to report to specific {@link StreamResultFuture}.
     *
     * @param streamResult result to report to
     * @return this object for chaining
     */
    public StreamSession register(StreamResultFuture streamResult)
    {
        this.streamResult = streamResult;
        return this;
    }

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    public void addStreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies)
    {
        requests.add(new StreamRequest(keyspace, ranges, columnFamilies));
    }

    /**
     * Set up transfer for specific keyspace/ranges/CFs
     *
     * @param keyspace Transfer keyspace
     * @param ranges Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     */
    public void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables)
    {
        Collection<ColumnFamilyStore> stores = new HashSet<>();
        // if columnfamilies are not specified, we add all cf under the keyspace
        if (columnFamilies.isEmpty())
        {
            stores.addAll(Table.open(keyspace).getColumnFamilyStores());
        }
        else
        {
            for (String cf : columnFamilies)
                stores.add(Table.open(keyspace).getColumnFamilyStore(cf));
        }

        if (flushTables)
            flushSSTables(stores);

        List<SSTableReader> sstables = Lists.newLinkedList();
        for (ColumnFamilyStore cfStore : stores)
        {
            List<AbstractBounds<RowPosition>> rowBoundsList = Lists.newLinkedList();
            for (Range<Token> range : ranges)
                rowBoundsList.add(range.toRowBounds());
            ColumnFamilyStore.ViewFragment view = cfStore.markReferenced(rowBoundsList);
            sstables.addAll(view.sstables);
        }
        addTransferFiles(ranges, sstables);
    }

    /**
     * Set up transfer of the specific SSTables.
     * {@code sstables} must be marked as referenced so that not get deleted until transfer completes.
     *
     * @param ranges Transfer ranges
     * @param sstables Transfer files
     */
    public void addTransferFiles(Collection<Range<Token>> ranges, Collection<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
            {
                // A reference was acquired on the sstable and we won't stream it
                sstable.releaseReference();
                continue;
            }
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            UUID cfId = sstable.metadata.cfId;
            StreamTransferTask task = transfers.get(cfId);
            if (task == null)
            {
                task = new StreamTransferTask(this, cfId);
                transfers.put(cfId, task);
            }
            task.addTransferFile(sstable, estimatedKeys, sections);
        }
    }

    /**
     * Start this stream session.
     * <p>
     * When this session is created on the initiator node,
     */
    public void run()
    {
        assert streamResult != null : "No operation is associated with this session";

        try
        {
            if (handler.isConnected())
            {
                handler.start();
            }
            else
            {
                if (requests.isEmpty() && transfers.isEmpty())
                {
                    logger.debug("Session does not have any tasks.");
                    streamResult.handleSessionComplete(this);
                }
                else
                {
                    handler.connect();
                }
            }
        }
        catch (IOException e)
        {
            onError(e);
        }
    }

    /**
     * Return if this session completed successfully.
     *
     * @return true if session completed successfully.
     */
    public boolean isSuccess()
    {
        return completed && peerCompleted;
    }

    public void messageReceived(StreamMessage message)
    {
        switch (message.type)
        {
            case PREPARE:
                PrepareMessage msg = (PrepareMessage) message;
                prepare(msg.requests, msg.summaries);
                break;

            case FILE:
                onFileReceive((FileMessage) message);
                break;

            case RETRY:
                RetryMessage retry = (RetryMessage) message;
                retry(retry.cfId, retry.sequenceNumber);
                break;

            case COMPLETE:
                complete();
                break;

            case SESSION_FAILED:
                onSessionFailedReceived();
                break;
        }
    }

    /**
     * Call back for connection success.
     *
     * When connected, session moves to preparing phase and sends prepare message.
     */
    public void onConnect()
    {
        state = State.PREPARING;

        logger.debug("Connected. Sending prepare...");
        // send prepare message
        PrepareMessage prepare = new PrepareMessage();
        prepare.requests.addAll(requests);
        for (StreamTransferTask task : transfers.values())
            prepare.summaries.add(task.getSummary());
        handler.sendMessage(prepare);

        // if we don't need to prepare for receiving stream, start sending files immediately
        if (requests.isEmpty())
        {
            logger.debug("Prepare complete. Start streaming files.");
            startStreamingFiles();
        }
    }

    /**
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    public void onError(Throwable e)
    {
        logger.error("onError", e);
        // send session failure message
        handler.sendMessage(new SessionFailedMessage());
        // fail session
        streamResult.handleSessionComplete(this);
    }

    /**
     * Prepare this session for sending/receiving files.
     */
    public void prepare(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {
        logger.debug("Start preparing this session (" + requests.size() + " requests, " + summaries.size() + " columnfamilies receiving)");
        // prepare tasks
        for (StreamRequest request : requests)
            addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        // send back prepare message if prepare message contains stream request
        if (!requests.isEmpty())
        {
            PrepareMessage prepare = new PrepareMessage();
            for (StreamTransferTask task : transfers.values())
                prepare.summaries.add(task.getSummary());
            handler.sendMessage(prepare);
        }

        maybeCompleted();

        logger.debug("Prepare complete. Start streaming files.");
        startStreamingFiles();
    }

    /**
     * Call back after sending FileMessageHeader.
     *
     * @param header sent header
     */
    public void onFileSend(FileMessageHeader header)
    {
        transfers.get(header.cfId).complete(header.sequenceNumber);
    }

    /**
     * Call back after receiving FileMessageHeader.
     *
     * @param message received file
     */
    public void onFileReceive(FileMessage message)
    {
        receivers.get(message.header.cfId).receive(message.sstable);
    }

    public void onStreamProgress(Descriptor desc, StreamEvent.Direction direction, long bytes, long total)
    {
        fireStreamEvent(new StreamEvent.ProgressEvent(this, desc.filenameFor(Component.DATA), direction, bytes, total));
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.RETRY} message.
     *
     * @param cfId ColumnFamily ID
     * @param sequenceNumber Sequence number to indicate which file to stream again
     */
    public void retry(UUID cfId, int sequenceNumber)
    {
        FileMessage message = transfers.get(cfId).createMessageForRetry(sequenceNumber);
        handler.sendMessage(message);
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    public synchronized void complete()
    {
        peerCompleted = true;
        logger.debug("complete this:" + completed + ", peer:" + peerCompleted);
        if (this.completed)
        {
            handler.close();
            streamResult.handleSessionComplete(this);
        }
    }

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    public synchronized void onSessionFailedReceived()
    {
        logger.debug("onSessionReceived");
        handler.close();
        streamResult.handleSessionComplete(this);
    }

    public void doRetry(FileMessageHeader header, Throwable e)
    {
        // retry
        retries++;
        if (retries > DatabaseDescriptor.getMaxStreamingRetries())
            onError(new IOException("Too many retries for " + header, e));
        else
            handler.sendMessage(new RetryMessage(header.cfId, header.sequenceNumber));
    }

    public List<StreamSummary> getReceivingSummaries()
    {
        List<StreamSummary> summaries = Lists.newArrayList();
        for (StreamTask receiver : receivers.values())
            summaries.add(receiver.getSummary());
        return summaries;
    }

    public List<StreamSummary> getTransferSummaries()
    {
        List<StreamSummary> summaries = Lists.newArrayList();
        for (StreamTask transfer : transfers.values())
            summaries.add(transfer.getSummary());
        return summaries;
    }

    public synchronized void taskCompleted(StreamReceiveTask completedTask)
    {
        receivers.remove(completedTask.cfId);
        maybeCompleted();
    }

    public synchronized void taskCompleted(StreamTransferTask completedTask)
    {
        transfers.remove(completedTask.cfId);
        maybeCompleted();
    }

    private void maybeCompleted()
    {
        logger.debug((receivers.size() + transfers.size()) + " remaining...");
        if (receivers.isEmpty() && transfers.isEmpty())
        {
            this.completed = true;
            logger.debug("onAllTaskComplete this:" + completed + ", peer:" + peerCompleted);
            handler.sendMessage(new CompleteMessage());
            if (peerCompleted)
            {
                handler.close();
                streamResult.handleSessionComplete(this);
            }
        }
    }

    public ByteBuffer createStreamInitMessage(int version)
    {
        StreamInitMessage message = new StreamInitMessage(streamResult.planId, streamResult.type);
        return message.createMessage(false, version);
    }

    protected void fireStreamEvent(StreamEvent event)
    {
        streamResult.fireStreamEvent(event);
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!endpoint.equals(peer))
            return;

        // We want a higher confidence in the failure detection than usual because failing a streaming wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        streamResult.handleSessionComplete(this);
    }

    /**
     * Flushes matching column families from the given keyspace, or all columnFamilies
     * if the cf list is empty.
     */
    private void flushSSTables(Iterable<ColumnFamilyStore> stores)
    {
        logger.info("Flushing memtables for {}...", stores);
        List<Future<?>> flushes = new ArrayList<>();
        for (ColumnFamilyStore cfs : stores)
            flushes.add(cfs.forceFlush());
        FBUtilities.waitOnFutures(flushes);
    }

    private void prepareReceiving(StreamSummary summary)
    {
        logger.debug("prepare receiving " + summary);
        if (summary.files > 0)
            receivers.put(summary.cfId, new StreamReceiveTask(this, summary.cfId, summary.files, summary.totalSize));
    }

    private void startStreamingFiles()
    {
        state = State.STREAMING;

        fireStreamEvent(new StreamEvent.SessionPreparedEvent(this));
        for (StreamTransferTask task : transfers.values())
        {
            if (task.getFileMessages().size() > 0)
                handler.sendMessages(task.getFileMessages());
            else
                taskCompleted(task);
        }
    }
}
