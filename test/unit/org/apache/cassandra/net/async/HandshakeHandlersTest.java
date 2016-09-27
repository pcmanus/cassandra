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
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.READY;
import static org.apache.cassandra.net.async.InboundHandshakeHandler.State.MESSAGING_HANDSHAKE_COMPLETE;
import static org.apache.cassandra.net.async.InboundHandshakeHandlerTest.SHH_HANDLER_NAME;

public class HandshakeHandlersTest
{
    private static final String KEYSPACE1 = "NettyPipilineTest";
    private static final String STANDARD1 = "Standard1";

    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9999);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private int receivedMessages;
    private final Consumer<MessageInWrapper> COUNTING_CONSUMER = messageInWrapper -> receivedMessages++;

    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));
        CompactionManager.instance.disableAutoCompaction();
        InboundHandshakeHandler.handshakeHandlerChannelHandlerName = SHH_HANDLER_NAME;
    }

    @Before
    public void setUp()
    {
        receivedMessages = 0;
    }

    @Test
    public void handshake_HappyPath()
    {
        // beacuse both CHH & SHH are ChannelInboundHandlers, we can't use the same EmbeddedChannel to handle them
        InboundHandshakeHandler inboundHandshakeHandler = new InboundHandshakeHandler(new TestAuthenticator(true));
        EmbeddedChannel serverChannel = new EmbeddedChannel(inboundHandshakeHandler);

        OutboundMessagingConnection imc = new OutboundMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, new FakeCoalescingStrategy(true));
        OutboundHandshakeHandler clientHandshakeHandler = new OutboundHandshakeHandler(REMOTE_ADDR, MESSAGING_VERSION, false, imc::finishHandshake, NettyFactory.Mode.MESSAGING);
        EmbeddedChannel clientChannel = new EmbeddedChannel(clientHandshakeHandler);
        Assert.assertEquals(1, clientChannel.outboundMessages().size());

        // move internode protocol Msg1 to the server's channel
        serverChannel.writeInbound(clientChannel.readOutbound());
        Assert.assertEquals(1, serverChannel.outboundMessages().size());

        // move internode protocol Msg2 to the client's channel
        clientChannel.writeInbound(serverChannel.readOutbound());
        Assert.assertEquals(1, clientChannel.outboundMessages().size());

        // move internode protocol Msg3 to the server's channel
        serverChannel.writeInbound(clientChannel.readOutbound());

        Assert.assertEquals(READY, imc.getState());
        Assert.assertEquals(MESSAGING_HANDSHAKE_COMPLETE, inboundHandshakeHandler.getState());
    }

    @Test
    public void lotsOfMutations_NoCompression() throws IOException
    {
        lotsOfMutations(false);
    }

    @Test
    public void lotsOfMutations_WithCompression() throws IOException
    {
        lotsOfMutations(true);
    }

    private void lotsOfMutations(boolean compress)
    {
        TestChannels channels = buildChannels(compress);
        EmbeddedChannel clientChannel = channels.clientChannel;
        EmbeddedChannel serverChannel = channels.serverChannel;

        // now the actual test!
        ByteBuffer buf = ByteBuffer.allocate(1 << 10);
        byte[] bytes = "ThisIsA16CharStr".getBytes();
        while (buf.remaining() > 0)
            buf.put(bytes);

        // write a bunch of messages to the channel
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        int count = 1024;
        for (int i = 0; i < count; i++)
        {
            if (i % 2 == 0)
            {
                Mutation mutation = new RowUpdateBuilder(cfs1.metadata, 0, "k")
                                    .clustering("bytes")
                                    .add("val", buf)
                                    .build();

                QueuedMessage msg = new QueuedMessage(mutation.createMessage(), i);
                clientChannel.writeAndFlush(msg);
            }
            else
            {
                clientChannel.writeAndFlush(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
            }
        }
        clientChannel.flush();

        // move the messages to the server channel
        Object o;
        while ((o = clientChannel.readOutbound()) != null)
            serverChannel.writeInbound(o);

        Assert.assertTrue(clientChannel.outboundMessages().isEmpty());
        // if compress, LZ4FrameEncoder will send 'close' packet to peer (thus a message is in the channel)
        Assert.assertEquals(compress, clientChannel.finishAndReleaseAll());
        Assert.assertFalse(serverChannel.finishAndReleaseAll());
    }

    private TestChannels buildChannels(boolean compress)
    {
        EmbeddedChannel clientChannel = new EmbeddedChannel(new OutboundHandshakeHandler(REMOTE_ADDR, MESSAGING_VERSION, compress, this::nop, NettyFactory.Mode.MESSAGING));
        OutboundMessagingConnection imc = new OutboundMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, new FakeCoalescingStrategy(false));
        imc.setTargetVersion(MESSAGING_VERSION);
        imc.setupPipeline(clientChannel.pipeline(), MESSAGING_VERSION, compress);
        // remove the client handshake message from the outbound messages
        clientChannel.outboundMessages().clear();

        EmbeddedChannel serverChannel = new EmbeddedChannel(new InboundHandshakeHandler(new TestAuthenticator(true)));
        InboundHandshakeHandler.setupMessagingPipeline(serverChannel.pipeline(), InboundHandshakeHandler.createHandlers(REMOTE_ADDR.getAddress(), compress, MESSAGING_VERSION, COUNTING_CONSUMER));

        return new TestChannels(clientChannel, serverChannel);
    }

    private static class TestChannels
    {
        final EmbeddedChannel clientChannel;
        final EmbeddedChannel serverChannel;

        TestChannels(EmbeddedChannel clientChannel, EmbeddedChannel serverChannel)
        {
            this.clientChannel = clientChannel;
            this.serverChannel = serverChannel;
        }
    }

    private Void nop(OutboundMessagingConnection.ConnectionHandshakeResult connectionHandshakeResult)
    {
        // do nothing, really
        return null;
    }
}
