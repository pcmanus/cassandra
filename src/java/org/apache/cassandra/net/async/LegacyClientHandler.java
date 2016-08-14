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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.channels.Channels;
import java.util.Queue;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.NIODataInputStream;
import org.apache.cassandra.net.IncomingTcpConnection;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection;

import static org.apache.cassandra.net.IncomingTcpConnection.BUFFER_SIZE;
import static org.apache.cassandra.net.IncomingTcpConnection.receiveMessage;

/**
 * Handles receiving messages from peers that are not on the framed version of the internode messaging protocol,
 * basically any peer < {@link MessagingService#VERSION_40}. As those peers do not send a message frame, there will be no
 * indication for the message size. Without the size, we have no idea how many bytes to expect for the message, and thus
 * non-blocking IO becomes dramtically more difficult and expensive in terms of resource utilitization.
 * Hence, you are stuck with using blocking IO and just having a thread sit on a socket waiting for bytes to come in,
 * the pre-cassandra 4.0 model.
 *
 * During a cluster upgrade, some nodes will be on the old protocol, and the upgraded nodes need to be able to
 * handle the previous protocol's behavior. This means we need some similar blocking IO beahvior (a thread sitting
 * on a socket waiting for bytes). The solution here, as it's in a netty context and can't just block indefinitely,
 * is to take the incoming {@link ByteBuf}s and put those onto a {@link Queue}. There is a background thread per-instance
 * that pulls from that queue via an intermediary {@link AppendingByteBufInputStream}, and ultimately passes it to
 * {@link IncomingTcpConnection#receiveMessage(InetAddress, DataInputPlus, int)}.
 *
 * Closing the channel will invoke {@link #close()}, which interrupts the {@link #blockingIOThread}.
 * If the {@link #blockingIOThread} is blocked waiting on data from the {@link #inputStream}, it will stop blocking and throw
 * an {@link InterruptedException}, which we catch. The {@link #closed} field is also updated.
 */
class LegacyClientHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyClientHandler.class);

    private final boolean compressed;
    private final int messagingVersion;
    private final AppendingByteBufInputStream inputStream;

    /**
     * The address of the node we are receiving messages from on this channel.
     */
    private final InetAddress peer;

    /**
     * The background thread that blocks for data.
     */
    private Thread blockingIOThread;
    private volatile boolean closed;

    LegacyClientHandler(InetAddress peer, boolean compressed, int messagingVersion)
    {
        this(peer, compressed, messagingVersion, new AppendingByteBufInputStream());
    }

    @VisibleForTesting
    LegacyClientHandler(InetAddress peer, boolean compressed, int messagingVersion, AppendingByteBufInputStream inputStream)
    {
        this.peer = peer;
        this.compressed = compressed;
        this.messagingVersion = messagingVersion;
        this.inputStream = inputStream;
    }

    @Override
    @SuppressWarnings("resource")
    public void channelActive(ChannelHandlerContext ctx)
    {
        blockingIOThread = new Thread(new BlockingIODeserialier(ctx, inputStream, compressed, messagingVersion));
        blockingIOThread.start();
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (!closed && msg instanceof ByteBuf)
        {
            try
            {
                inputStream.append((ByteBuf) msg);
            }
            catch(IllegalStateException ise)
            {
                closed = true;
            }
        }
        else
        {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    void close()
    {
        if (closed)
            return;

        closed = true;
        if (blockingIOThread != null)
            blockingIOThread.interrupt();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing internode messages", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

    @VisibleForTesting
    void setClosed(boolean closed)
    {
        this.closed = closed;
    }

    private final class BlockingIODeserialier implements Runnable
    {
        private final ChannelHandlerContext ctx;

        // keep a reference to this so we can explicitly make sure to close() it
        private final InputStream byteBufInputStream;

        private final int messagingVersion;
        private final DataInputPlus in;

        @SuppressWarnings("resource")
        BlockingIODeserialier(ChannelHandlerContext ctx, InputStream byteBufInputStream, boolean compressed, int messagingVersion)
        {
            this.ctx = ctx;
            this.byteBufInputStream = byteBufInputStream;
            this.messagingVersion = messagingVersion;
            if (compressed)
            {
                LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
                Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(OutboundTcpConnection.LZ4_HASH_SEED).asChecksum();
                in = new DataInputPlus.DataInputStreamPlus(new LZ4BlockInputStream(byteBufInputStream, decompressor, checksum));
            }
            else
            {
                in = new NIODataInputStream(Channels.newChannel(byteBufInputStream), BUFFER_SIZE);
            }
        }

        @Override
        public void run()
        {
            boolean notifyHandlerOnClose = true;
            try
            {
                while (!closed)
                {
                    MessagingService.validateMagic(in.readInt());
                    receiveMessage(peer, in, messagingVersion);
                }
            }
            catch (EOFException e)
            {
                logger.trace("eof reading from socket; closing", e);
                notifyHandlerOnClose = false;
            }
            catch (UnknownColumnFamilyException e)
            {
                logger.warn("UnknownColumnFamilyException reading from socket; closing", e);
            }
            catch (IOException e)
            {
                logger.trace("IOException reading from socket; closing", e);
            }
            finally
            {
                if (notifyHandlerOnClose)
                {
                    LegacyClientHandler.this.closed = true;
                    ctx.close();
                }
                FileUtils.closeQuietly(byteBufInputStream);
            }
        }
    }
}
