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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * StreamResultFuture asynchronously returns the final {@link StreamState} of execution of {@link StreamPlan}.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to track progress of the streaming.
 */
public final class StreamResultFuture extends AbstractFuture<StreamState>
{
    public final UUID planId;
    public final String description;
    private final List<StreamEventHandler> eventListeners = Collections.synchronizedList(new ArrayList<StreamEventHandler>());
    private final AtomicInteger remainingSession;
    private final Map<InetAddress, SessionInfo> sessionStates = new NonBlockingHashMap<>();

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     * @param numberOfSessions number of sessions to wait for complete
     */
    StreamResultFuture(UUID planId, String description, int numberOfSessions)
    {
        this.planId = planId;
        this.description = description;
        this.remainingSession = new AtomicInteger(numberOfSessions);
        // if there is no session to listen to, we immediately set result for returning
        if (numberOfSessions == 0)
            set(getCurrentState());
    }

    public void addEventListener(StreamEventHandler listener)
    {
        Futures.addCallback(this, listener);
        eventListeners.add(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    public StreamState getCurrentState()
    {
        return new StreamState(planId, description, ImmutableSet.copyOf(sessionStates.values()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamResultFuture that = (StreamResultFuture) o;
        return planId.equals(that.planId);
    }

    @Override
    public int hashCode()
    {
        return planId.hashCode();
    }

    void handleSessionPrepared(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo);
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        maybeComplete();
    }

    public void handleProgress(ProgressInfo progress)
    {
        sessionStates.get(progress.peer).updateProgress(progress);
        fireStreamEvent(new StreamEvent.ProgressEvent(planId, progress));
    }

    void fireStreamEvent(StreamEvent event)
    {
        // delegate to listener
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }

    private void maybeComplete()
    {
        if (remainingSession.decrementAndGet() == 0)
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
                setException(new StreamException(finalState, "Stream failed"));
            else
                set(finalState);
        }
    }
}
