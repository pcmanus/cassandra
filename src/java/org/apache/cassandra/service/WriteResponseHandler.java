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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.net.MessageIn;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractRequestCallback<WriteResponse, Void>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected final Table table;
    public final Collection<InetAddress> naturalEndpoints;
    public final Collection<InetAddress> pendingEndpoints;
    protected final ConsistencyLevel consistencyLevel;
    private final WriteType writeType;

    /**
     * @param pendingEndpoints
     * @param callback A callback to be called when the write is successful.
     */
    public WriteResponseHandler(Table table,
                                   Collection<InetAddress> naturalEndpoints,
                                   Collection<InetAddress> pendingEndpoints,
                                   ConsistencyLevel consistencyLevel,
                                   WriteType writeType)
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        super(consistencyLevel.blockFor(table) + pendingEndpoints.size());

        this.table = table;
        this.pendingEndpoints = pendingEndpoints;
        this.naturalEndpoints = naturalEndpoints;
        this.consistencyLevel = consistencyLevel;
        this.writeType = writeType;
    }

    public Iterable<InetAddress> allEndpoints()
    {
        return Iterables.concat(naturalEndpoints, pendingEndpoints);
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType)
    {
        this(null, Arrays.asList(endpoint), Collections.<InetAddress>emptyList(), ConsistencyLevel.ONE, writeType);
    }

    protected boolean process(MessageIn<WriteResponse> message)
    {
        return true;
    }

    protected Void getResult()
    {
        return null;
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(table, Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive));
    }

    public RequestTimeoutException reportTimeout()
    {
        return new WriteTimeoutException(writeType, consistencyLevel, getResponsesCount(), waitFor());
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

}
