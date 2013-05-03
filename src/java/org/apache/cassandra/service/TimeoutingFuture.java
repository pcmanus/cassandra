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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.TimerTask;
import org.jboss.netty.util.Timeout;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.metrics.ClientRequestMetrics;

/**
 * Wraps a request tracker to:
 *   1. handle the timeout corresponding to the query
 *   2. record the execution time
 */
class TimeoutingFuture<T> extends AbstractFuture<T> implements TimerTask
{
    private static final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("Timeouter-%d").build());

    private final RequestType type;
    private volatile RequestTracker<T> tracker;
    private final long startTime;

    private final Timeout timeout;

    private TimeoutingFuture(RequestType type)
    {
        this.type = type;
        this.startTime = System.nanoTime();

        // Set up the request to timeout
        this.timeout = timer.newTimeout(this, type.getTimeoutInMs(), TimeUnit.MILLISECONDS);
    }

    public static <T> TimeoutingFuture<T> create(RequestType type)
    {
        return new TimeoutingFuture(type);
    }

    public TimeoutingFuture<T> register(RequestTracker<T> tracker)
    {
        assert this.tracker == null;
        this.tracker = tracker;

        // Register a callback to trigger this future once the tracked one return
        FutureCallback<T> callback = new FutureCallback<T>()
        {
            public void onSuccess(T value)
            {
                TimeoutingFuture.this.set(value);
            }

            public void onFailure(Throwable t)
            {
                if (t instanceof UnavailableException)
                    type.reportUnavailable();
                else if (t instanceof OverloadedException)
                    type.reportOverloaded();
                TimeoutingFuture.this.setException(t);
            }
        };
        Futures.addCallback(tracker.future(), callback);
        return this;
    }

    @Override
    protected boolean set(T value)
    {
        if (!super.set(value))
            return false;

        type.addRequestLatency(startTime);
        timeout.cancel();
        return true;
    }

    @Override
    protected boolean setException(Throwable t)
    {
        if (!super.setException(t))
            return false;

        type.addRequestLatency(startTime);
        timeout.cancel();
        return true;
    }

    public void setUnavailable()
    {
        cancel(true);
        type.reportUnavailable();
    }

    public void run(Timeout timeout)
    {
        // We start the timer before having registered the tracker, so in theory it's possible
        // to timeout before the tracker has been set. In practice, that won't happen unless
        // the rpc timeout is way too low, but instead of failing, let's just send a timeout that
        // says no node answered (the CL and WriteType are wrong in the exception returned but that
        // likely doesn't matter).
        if (tracker != null)
        {
            setException(type == RequestType.WRITE
                         ? new WriteTimeoutException(WriteType.SIMPLE, ConsistencyLevel.ONE, 0, Integer.MAX_VALUE)
                         : new ReadTimeoutException(ConsistencyLevel.ONE, 0, Integer.MAX_VALUE, false));
            return;
        }

        RequestTimeoutException ex = tracker.reportTimeout();

        // If the exception is null, it just mean we raced with normal completion, which is fine,
        // we just ignore the timeout.
        if (ex == null)
            return;

        setException(ex);
        type.reportTimeout();
    }

    // Note that cancelling do not register the operation latency
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        if (!(super.cancel(mayInterruptIfRunning)))
            return false;

        // Cancel the timeout and the underlying future
        timeout.cancel();
        if (tracker != null)
            tracker.future().cancel(mayInterruptIfRunning);
        return true;
    }
}
