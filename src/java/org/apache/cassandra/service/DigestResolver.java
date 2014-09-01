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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.net.MessageIn;

public class DigestResolver extends AbstractResolver
{
    private volatile ReadResponse dataResponse;

    public DigestResolver(Keyspace keyspace, CFMetaData metadata, ConsistencyLevel consistency, int nowInSec, int maxResponseCount)
    {
        super(keyspace, metadata, consistency, nowInSec, maxResponseCount);
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        if (dataResponse == null && !message.payload.isDigestQuery())
            dataResponse = message.payload;
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public DataIterator getData()
    {
        assert isDataPresent();
        return PartitionIterators.asDataIterator(dataResponse.makeIterator(), nowInSec);
    }

    /*
     * This method handles two different scenarios:
     *
     * a) we're handling the initial read of data from the closest replica + digests
     *    from the rest. In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * b) we're checking additional digests that arrived after the minimum to handle
     *    the requested ConsistencyLevel, i.e. asynchronous read repair check
     */
    public DataIterator resolve() throws DigestMismatchException
    {
        if (logger.isDebugEnabled())
            logger.debug("resolving {} responses", responses.size());

        long start = System.nanoTime();

        // validate digests against each other; throw immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest = response.digest();
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                throw new DigestMismatchException(dataResponse.makeIterator().next().partitionKey(), digest, newDigest);
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return PartitionIterators.asDataIterator(dataResponse.makeIterator(), nowInSec);
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
