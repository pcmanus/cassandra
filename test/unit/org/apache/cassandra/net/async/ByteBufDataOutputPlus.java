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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;

public class ByteBufDataOutputPlus extends ByteBufOutputStream implements DataOutputPlus
{
    ByteBufDataOutputPlus(ByteBuf buffer)
    {
        super(buffer);
    }

    public void write(ByteBuffer buffer) throws IOException
    {
        buffer().writeBytes(buffer);
    }

    public void write(Memory memory, long offset, long length) throws IOException
    {
        buffer().writeBytes(memory.asByteBuffer(offset, (int)length));
    }

    public <R> R applyToChannel(com.google.common.base.Function<WritableByteChannel, R> c) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
