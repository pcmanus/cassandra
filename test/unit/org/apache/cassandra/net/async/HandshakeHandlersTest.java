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
import java.util.Optional;
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
        EmbeddedChannel inboundChannel = new EmbeddedChannel(inboundHandshakeHandler);

        OutboundMessagingConnection imc = new OutboundMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, Optional.of(new FakeCoalescingStrategy(true)));
        OutboundHandshakeHandler clientHandshakeHandler = new OutboundHandshakeHandler(REMOTE_ADDR, MESSAGING_VERSION, false, imc::finishHandshake, NettyFactory.Mode.MESSAGING);
        EmbeddedChannel outboundChannel = new EmbeddedChannel(clientHandshakeHandler);
        Assert.assertEquals(1, outboundChannel.outboundMessages().size());

        // move internode protocol Msg1 to the server's channel
        Object o;
        while ((o = outboundChannel.readOutbound()) != null)
            inboundChannel.writeInbound(o);
        Assert.assertEquals(1, inboundChannel.outboundMessages().size());

        // move internode protocol Msg2 to the client's channel
        while ((o = inboundChannel.readOutbound()) != null)
            outboundChannel.writeInbound(o);
        Assert.assertEquals(1, outboundChannel.outboundMessages().size());

        // move internode protocol Msg3 to the server's channel
        while ((o = outboundChannel.readOutbound()) != null)
            inboundChannel.writeInbound(o);

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
        EmbeddedChannel outboundChannel = channels.outboundChannel;
        EmbeddedChannel inboundChannel = channels.inboundChannel;

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
                outboundChannel.writeAndFlush(msg);
            }
            else
            {
                outboundChannel.writeAndFlush(new QueuedMessage(new MessageOut<>(MessagingService.Verb.ECHO), i));
            }
        }
        outboundChannel.flush();

        // move the messages to the other channel
        Object o;
        while ((o = outboundChannel.readOutbound()) != null)
            inboundChannel.writeInbound(o);

        Assert.assertTrue(outboundChannel.outboundMessages().isEmpty());
        // if compress, LZ4FrameEncoder will send 'close' packet to peer (thus a message is in the channel)
        Assert.assertEquals(compress, outboundChannel.finishAndReleaseAll());
        Assert.assertFalse(inboundChannel.finishAndReleaseAll());
    }

    private TestChannels buildChannels(boolean compress)
    {
        EmbeddedChannel outboundChannel = new EmbeddedChannel(new OutboundHandshakeHandler(REMOTE_ADDR, MESSAGING_VERSION, compress, this::nop, NettyFactory.Mode.MESSAGING));
        OutboundMessagingConnection imc = new OutboundMessagingConnection(REMOTE_ADDR, LOCAL_ADDR, null, Optional.of(new FakeCoalescingStrategy(false)));
        imc.setTargetVersion(MESSAGING_VERSION);
        imc.setupPipeline(outboundChannel.pipeline(), MESSAGING_VERSION, compress);
        // remove the client handshake message from the outbound messages
        outboundChannel.outboundMessages().clear();

        EmbeddedChannel inboundChannel = new EmbeddedChannel(new InboundHandshakeHandler(new TestAuthenticator(true)));
        InboundHandshakeHandler.setupMessagingPipeline(inboundChannel.pipeline(), InboundHandshakeHandler.createHandlers(REMOTE_ADDR.getAddress(), compress, MESSAGING_VERSION, COUNTING_CONSUMER));

        return new TestChannels(outboundChannel, inboundChannel);
    }

    private static class TestChannels
    {
        final EmbeddedChannel outboundChannel;
        final EmbeddedChannel inboundChannel;

        TestChannels(EmbeddedChannel outboundChannel, EmbeddedChannel inboundChannel)
        {
            this.outboundChannel = outboundChannel;
            this.inboundChannel = inboundChannel;
        }
    }

    private Void nop(OutboundMessagingConnection.ConnectionHandshakeResult connectionHandshakeResult)
    {
        // do nothing, really
        return null;
    }
}
