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
package org.apache.cassandra.cql3.transport;

import java.util.concurrent.TimeUnit;

import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.DefaultObjectSizeEstimator;
import org.jboss.netty.util.ObjectSizeEstimator;

import org.apache.cassandra.concurrent.NamedThreadFactory;

public class RequestThreadPoolExecutor extends OrderedMemoryAwareThreadPoolExecutor
{
    private final static int CORE_THREAD_TIMEOUT_SEC = 30;
    private final static int MAXIMUM_IN_FLIGHT_REQUEST = 65535;

    // A trivial estimator that always return 1.
    // Estimating memory usage in C* is much more complex than just estimating
    // the size of the request themselves, so don't bother spending time on that.
    // Though maybe we could do better estimate, like distinguishing between
    // request doing reads vs writes etc...
    private static final ObjectSizeEstimator estimator = new DefaultObjectSizeEstimator()
    {
        @Override
        public int estimateSize(Object o)
        {
            return 1;
        }
    };

    public RequestThreadPoolExecutor()
    {
        // XXX: maybe we could make some of those configurable
        super(16 * Runtime.getRuntime().availableProcessors(),
              Integer.MAX_VALUE,
              MAXIMUM_IN_FLIGHT_REQUEST,
              CORE_THREAD_TIMEOUT_SEC, TimeUnit.SECONDS,
              estimator,
              new NamedThreadFactory("Native-Transport-Executor"));
    }
}
