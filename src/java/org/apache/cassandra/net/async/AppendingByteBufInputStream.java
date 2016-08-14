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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * An {@link InputStream} that blocks on a {@link #queue} for {@link ByteBuf}s. An instance is responsibile for the reference
 * counting of any {@link ByteBuf}s passed to {@link #append(ByteBuf)}.
 */
public class AppendingByteBufInputStream extends InputStream
{
    private final byte[] oneByteArray = new byte[1];
    private final BlockingQueue<ByteBuf> queue;
    private ByteBuf currentBuf;
    private volatile boolean closed;

    public AppendingByteBufInputStream()
    {
        queue = new LinkedBlockingQueue<>();
    }

    public void append(ByteBuf buf) throws IllegalStateException
    {
        if (closed)
        {
            ReferenceCountUtil.release(buf);
            throw new IllegalStateException("stream is already closed, so cannot add another buffer");
        }
        queue.add(buf);
    }

    @Override
    public int read() throws IOException
    {
        int result = read(oneByteArray, 0, 1);
        if (result == 1)
            return oneByteArray[0] & 0xFF;
        if (result == -1)
            return -1;

        throw new IOException("failed to read from stream");
    }

    public int read(byte out[], int off, final int len) throws IOException
    {
        if (out == null)
            throw new NullPointerException();
        else if (off < 0 || len < 0 || len > out.length - off)
            throw new IndexOutOfBoundsException();
        else if (len == 0)
            return 0;

        int remaining = len;
        while (true)
        {
            if (currentBuf != null)
            {
                if (currentBuf.isReadable())
                {
                    int toReadCount = Math.min(remaining, currentBuf.readableBytes());
                    currentBuf.readBytes(out, off, toReadCount);
                    remaining -= toReadCount;

                    if (remaining == 0)
                        return len;
                    off += toReadCount;
                }

                currentBuf.release();
                currentBuf = null;
            }

            try
            {
                currentBuf = queue.take();
            }
            catch (InterruptedException e)
            {
                // we get notified (via interrupt) when the netty channel closes.
                throw new EOFException();
            }
        }
    }

    @VisibleForTesting
    public int buffersInQueue()
    {
        return queue.size();
    }

    @Override
    public void close()
    {
        if (closed)
            return;
        closed = true;

        if (currentBuf != null)
        {
            currentBuf.release();
            currentBuf = null;
        }

        ByteBuf buf;
        while ((buf = queue.poll()) != null)
            buf.release();
    }
}
