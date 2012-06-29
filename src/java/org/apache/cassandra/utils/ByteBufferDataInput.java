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
package org.apache.cassandra.utils;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * An implementation of the DataInputStream interface reading from a ByteBuffer
 * that allows reading a ByteBuffer without copy.
 *
 * This class is thread unsafe.
 */
public class ByteBufferDataInput extends DataInputStream
{
    public ByteBufferDataInput(ByteBuffer buffer)
    {
        super(new InputBuffer(buffer));
    }

    public ByteBuffer readBytes(int len)
    {
        return ((InputBuffer)in).readBytes(len);
    }

    public ByteBuffer buffer()
    {
        return ((InputBuffer)in).buffer;
    }

    public static class InputBuffer extends InputStream
    {
        private final ByteBuffer buffer;

        public InputBuffer(ByteBuffer bb)
        {
            this.buffer = bb.duplicate();
        }

        public int read() throws IOException
        {
            if (!buffer.hasRemaining())
                return -1;

            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException
        {
            if (!buffer.hasRemaining())
                return -1;

            len = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, len);
            return len;
        }

        @Override
        public int available() throws IOException
        {
            return buffer.remaining();
        }

        /**
         * Reads a ByteBuffer containing the next @param len bytes of this InputBuffer.
         *
         * No copy is performed by this operation, the returned ByteBuffer will share the
         * content of the underlying buffer.
         *
         * @throws IllegalArgumentException is this input buffer doesn't have
         * enough bytes remaining.
         */
        public ByteBuffer readBytes(int len)
        {
            if (buffer.remaining() < len)
                throw new IllegalArgumentException();

            ByteBuffer b = buffer.slice();
            b.limit(len);
            buffer.position(buffer.position() + len);
            return b;
        }
    }
}
