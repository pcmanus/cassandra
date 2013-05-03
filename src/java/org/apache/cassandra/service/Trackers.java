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
import java.util.Collection;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.exceptions.RequestTimeoutException;

public abstract class Trackers
{
    private Trackers() {}

    public static <T> RequestTracker<T> of(ListenableFuture<T> future, TimeoutReporter reporter)
    {
        return new SimpleTracker(future, reporter);
    }

    public static <T> RequestTracker<List<T>> combine(List<? extends RequestTracker<T>> trackers)
    {
        List<ListenableFuture<T>> futures = new ArrayList<ListenableFuture<T>>(trackers.size());
        List<TimeoutReporter> reporters = new ArrayList<TimeoutReporter>(trackers.size());

        for (RequestTracker<T> tracker : trackers)
        {
            futures.add(tracker.future());
            reporters.add(tracker);
        }
        return new SimpleTracker(Futures.allAsList(futures), new CombinedReporter(reporters));
    }

    public static <T, V> ListenableFuture<V> transform(RequestTracker<T> tracker, TimeoutReporter.Updatable globalReporter, AsyncFunction<T, V> fun)
    {
        globalReporter.switchTo(tracker);
        return Futures.transform(tracker.future(), fun);
    }

    public static <T, V> ListenableFuture<V> transform(RequestTracker<T> tracker, TimeoutReporter.Updatable globalReporter, Function<T, V> fun)
    {
        globalReporter.switchTo(tracker);
        return Futures.transform(tracker.future(), fun);
    }

    private static class SimpleTracker<T> implements RequestTracker<T>
    {
        private final ListenableFuture<T> future;
        private final TimeoutReporter reporter;

        private SimpleTracker(ListenableFuture<T> future, TimeoutReporter reporter)
        {
            this.future = future;
            this.reporter = reporter;
        }

        public ListenableFuture<T> future()
        {
            return future;
        }

        public RequestTimeoutException reportTimeout()
        {
            return reporter.reportTimeout();
        }
    }

    private static class CombinedReporter implements TimeoutReporter
    {
        private final Collection<TimeoutReporter> reporters;

        private CombinedReporter(Collection<TimeoutReporter> reporters)
        {
            this.reporters = reporters;
        }

        public RequestTimeoutException reportTimeout()
        {
            for (TimeoutReporter reporter : reporters)
            {
                RequestTimeoutException ex = reporter.reportTimeout();
                if (ex != null)
                    return ex;
            }
            return null;
        }
    }
}
