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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.tracing.Tracing;

public enum RequestType
{
    WRITE("Write"),
    READ ("Read"),
    RANGE("RangeSlice"),
    TRUNCATE("Truncate"),
    CAS_WRITE("ConditionalWrite"),
    CAS_READ("ConditionalRead");

    private final String name;
    public final ClientRequestMetrics metrics;

    private RequestType(String name)
    {
        this.name = name;
        this.metrics = new ClientRequestMetrics(name);
    }

    public long getTimeoutInMs()
    {
        switch (this)
        {
            case WRITE:
                return DatabaseDescriptor.getWriteRpcTimeout();
            case READ:
                return DatabaseDescriptor.getReadRpcTimeout();
            case RANGE:
                return DatabaseDescriptor.getRangeRpcTimeout();
            case TRUNCATE:
                return DatabaseDescriptor.getTruncateRpcTimeout();
            case CAS_WRITE:
            case CAS_READ:
                return DatabaseDescriptor.getCasRpcTimeout();
        }
        throw new AssertionError();
    }

    public void addRequestLatency(long startTimeNano)
    {
        this.metrics.addNano(System.nanoTime() - startTimeNano);
    }

    public void reportTimeout()
    {
        metrics.timeouts.mark();
        Tracing.trace(name + " timeout");
        // For old code
        switch (this)
        {
            case WRITE:
                ClientRequestMetrics.writeTimeouts.inc();
                break;
            case READ:
            case RANGE:
                ClientRequestMetrics.readTimeouts.inc();
                break;
        }
    }

    public void reportUnavailable()
    {
        metrics.unavailables.mark();
        Tracing.trace("Unavailable");
        // For old code
        switch (this)
        {
            case WRITE:
                ClientRequestMetrics.writeUnavailables.inc();
                break;
            case READ:
            case RANGE:
                ClientRequestMetrics.readUnavailables.inc();
                break;
        }
    }

    public void reportOverloaded()
    {
        metrics.unavailables.mark();
        Tracing.trace("Overloaded");
        // For old code
        switch (this)
        {
            case WRITE:
                ClientRequestMetrics.writeUnavailables.inc();
                break;
            case READ:
            case RANGE:
                ClientRequestMetrics.readUnavailables.inc();
                break;
        }
    }
}
