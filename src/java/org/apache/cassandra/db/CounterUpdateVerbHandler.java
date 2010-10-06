/**
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

import java.io.*;
import java.util.concurrent.TimeoutException;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;

public class CounterUpdateVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(CounterUpdateVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] bytes = message.getMessageBody();
        ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);

        try
        {
            DataInputStream is = new DataInputStream(buffer);
            CounterMutation cm = CounterMutation.serializer().deserialize(is);
            if (logger_.isDebugEnabled())
              logger_.debug("Applying forwarded " + cm);

            boolean success;
            try
            {
                StorageProxy.updateCounterOnReplica(cm);
                success = true;
            }
            catch (UnavailableException e)
            {
                success = false;
            }
            WriteResponse response = new WriteResponse(cm.getTable(), cm.key(), success);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            if (logger_.isDebugEnabled())
                logger_.debug("Forwarded " + cm + (success ? " applied" : "failed") + ".  Sending response to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(responseMessage, message.getFrom());
        }
        catch (TimeoutException e)
        {
            // The coordinator node will have timeout itself so we let that goes
        }
        catch (IOException e)
        {
            logger_.error("Error in counter mutation", e);
        }
    }
}
