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
package org.apache.cassandra.db;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;

public class CounterMutationVerbHandler implements IVerbHandler<CounterMutation>
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutationVerbHandler.class);

    public void doVerb(final MessageIn<CounterMutation> message, final int id)
    {
        try
        {
            final CounterMutation cm = message.payload;
            if (logger.isDebugEnabled())
              logger.debug("Applying forwarded " + cm);

            String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            // We should not wait for the result of the write in this thread,
            // otherwise we could have a distributed deadlock between replicas
            // running this VerbHandler (see #4578).
            // Instead, we add a listener to send the response.
            ListenableFuture<Void> future = StorageProxy.applyCounterMutationOnLeader(cm, localDataCenter).future();
            Futures.addCallback(future, new FutureCallback<Void>()
            {
                public void onSuccess(Void v)
                {
                    WriteResponse response = new WriteResponse();
                    MessagingService.instance().sendReply(response.createMessage(), id, message.from);
                }

                public void onFailure(Throwable t)
                {
                    // It's ok to ignore failure, the coordinator will timeout on it's own.
                }
            });
        }
        catch (RequestExecutionException e)
        {
            // The coordinator will timeout on it's own so ignore
            logger.debug("counter error", e);
        }
    }
}
