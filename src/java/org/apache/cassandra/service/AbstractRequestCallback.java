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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.RequestTracker;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractRequestCallback<M, V> extends AbstractFuture<V> implements IAsyncCallback<M>, RequestTracker<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRequestCallback.class);

    private final int waitFor;
    private final AtomicInteger responsesCount = new AtomicInteger(0);

    public AbstractRequestCallback(int waitFor)
    {
        this.waitFor = waitFor;
    }

    // If returns true, the message will be accounted for, otherwise, it won't
    protected abstract boolean process(MessageIn<M> message);

    // Called when all messaged waited on have been received to return the result
    protected abstract V getResult() throws Exception;

    // For overriding class. Allow to enforce a custom condition even when
    // 'waitFor' responses has been received.
    protected boolean isReady()
    {
        return true;
    }

    public void response(MessageIn<M> msg)
    {
        if (process(msg))
            responsesCount.incrementAndGet();

        if (responsesCount.get() >= waitFor && isReady())
        {
            try
            {
                set(getResult());
            }
            catch (Exception e)
            {
                setException(e);
            }
        }
    }

    public int getResponsesCount()
    {
        return responsesCount.get();
    }

    public int waitFor()
    {
        return waitFor;
    }

    public ListenableFuture<V> future()
    {
        return this;
    }
}
