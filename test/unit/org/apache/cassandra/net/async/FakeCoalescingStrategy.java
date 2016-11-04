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

package org.apache.cassandra.net.async;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.CoalescingStrategies;

class FakeCoalescingStrategy extends CoalescingStrategies.AbstractCoalescingStrategy
{
    private final boolean coalesces;
    boolean coalesceCallbackInvoked;

    FakeCoalescingStrategy(boolean coalesces)
    {
        super(null, LoggerFactory.getLogger(FakeCoalescingStrategy.class), "FakeCoalescingStrategy");
        this.coalesces = coalesces;
    }

    protected <C extends CoalescingStrategies.Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out, int maxItems) throws InterruptedException
    {
        throw new UnsupportedOperationException("should not get here in async code!");
    }

    public Consumer<Long> coalesceNonBlockingCallback()
    {
        return timestamp -> {
            coalesceCallbackInvoked = true;
        };
    }

    public long coalesceNonBlocking(long sampleTimestampNanos, int pendingMessageCount)
    {
        return 0;
    }

    public boolean isCoalescing()
    {
        return coalesces;
    }
}
