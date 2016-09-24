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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.MessageReceiveHandler.MessageHeader;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;

public class MessageReceiveHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final int MSG_VERSION = MessagingService.current_version;

    private static final int MSG_ID = 42;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (buf != null)
            buf.release();
    }

    @Test
    public void decode_HappyPath_NoParameters() throws Exception
    {
        MessageInWrapper result = decode_HappyPath(Collections.emptyMap());
        Assert.assertTrue(result.messageIn.parameters.isEmpty());

    }

    @Test
    public void decode_HappyPath_WithParameters() throws Exception
    {
        Map<String, byte[]> parameters = new HashMap<>();
        parameters.put("p1", "val1".getBytes(Charsets.UTF_8));
        parameters.put("p2", "val2".getBytes(Charsets.UTF_8));
        MessageInWrapper result = decode_HappyPath(parameters);
        Assert.assertEquals(2, result.messageIn.parameters.size());
    }

    private MessageInWrapper decode_HappyPath(Map<String, byte[]> parameters) throws Exception
    {
        MessageOut msgOut = new MessageOut(MessagingService.Verb.ECHO);
        for (Map.Entry<String, byte[]> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());
        serialize(msgOut);

        MessageReceiveHandler handler = new MessageReceiveHandler(addr.getAddress(), MSG_VERSION);
        List<Object> out = new ArrayList<>(2);
        handler.decode(null, buf, out);

        Assert.assertEquals(1, out.size());
        MessageInWrapper result = (MessageInWrapper) out.get(0);
        Assert.assertEquals(MSG_ID, result.id);
        Assert.assertEquals(msgOut.from, result.messageIn.from);
        Assert.assertEquals(msgOut.verb, result.messageIn.verb);

        return result;
    }

    private void serialize(MessageOut msgOut) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(MSG_ID); // this is the id
        buf.writeInt((int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime()));

        msgOut.serialize(new ByteBufDataOutputPlus(buf), MSG_VERSION);
    }


    @Test
    public void decode_WithHalfReceivedParameters() throws Exception
    {
        MessageOut msgOut = new MessageOut(MessagingService.Verb.ECHO);
            msgOut = msgOut.withParameter("p3", "val1".getBytes(Charsets.UTF_8));

        serialize(msgOut);

        // move the write index pointer back a few bytes to simulate like the full bytes are not present.
        // yeah, it's lame, but it tests the basics of what is happening during the deserialiization
        int originalWriterIndex = buf.writerIndex();
        buf.writerIndex(originalWriterIndex - 6);

        MessageReceiveHandler handler = new MessageReceiveHandler(addr.getAddress(), MSG_VERSION);
        List<Object> out = new ArrayList<>(2);
        handler.decode(null, buf, out);
        Assert.assertTrue(out.isEmpty());
        MessageHeader header = handler.getMessageHeader();
        Assert.assertEquals(MSG_ID, header.messageId);
        Assert.assertEquals(msgOut.verb, header.verb);
        Assert.assertEquals(msgOut.from, header.from);

        // now, set the writer index back to the original value to pretend that we actually got more bytes in
        buf.writerIndex(originalWriterIndex);
        handler.decode(null, buf, out);
        Assert.assertFalse(out.isEmpty());
    }
}
