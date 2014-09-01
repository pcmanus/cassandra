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

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    // A ReadResponse is either a data response (stored in data) or a digest response (stored in digest).
    private final PartitionIteratorHolder data;
    private final ByteBuffer digest;

    private ReadResponse(PartitionIteratorHolder data, ByteBuffer digest)
    {
        this.data = data;
        this.digest = digest;
    }

    public static ReadResponse create(PartitionIterator data, boolean isDigestResponse)
    {
        if (isDigestResponse)
            return new ReadResponse(null, digest(data));

        return new ReadResponse(new SimplePartitionIteratorHolder(data), null);
    }

    private static ByteBuffer digest(PartitionIterator iter)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        PartitionIterators.digest(iter, digest);
        return ByteBuffer.wrap(digest.digest());
    }

    public PartitionIterator makeIterator()
    {
        return data.makeIterator();
    }

    public ByteBuffer digest()
    {
        return isDigestQuery()
             ? digest
             : digest(makeIterator());
    }

    public boolean isDigestQuery()
    {
        return digest != null;
    }

    /*
     * For a given responses, we might have to call the 'makeIterator' function multiple
     * times for data responses due to digest computation (we call it first to compute the
     * digest and compare it to other responses, and a 2nd time to get the actual data).
     * The following class allow is a hack to deal with this without complicating things
     * further (there might be better solution in the long run). This is not to be exposed
     * outside of this class.
     */
    private static abstract class PartitionIteratorHolder
    {
        public abstract PartitionIterator makeIterator();
    }

    private static class SimplePartitionIteratorHolder extends PartitionIteratorHolder
    {
        private final PartitionIterator iter;

        public SimplePartitionIteratorHolder(PartitionIterator iter)
        {
            this.iter = iter;
        }

        public PartitionIterator makeIterator()
        {
            return iter;
        }
    }

    private static class BufferedPartitionIteratorHolder extends PartitionIteratorHolder
    {
        private final List<Partition> data;

        public BufferedPartitionIteratorHolder(PartitionIterator iter)
        {
            if (!iter.hasNext())
            {
                data = Collections.emptyList();
            }
            else
            {
                AtomIterator atoms = iter.next();
                if (!iter.hasNext())
                {
                    data = Collections.<Partition>singletonList(ArrayBackedPartition.accumulate(atoms));
                }
                else
                {
                    data = new ArrayList<>();
                    data.add(ArrayBackedPartition.accumulate(atoms));
                    while (iter.hasNext())
                        data.add(ArrayBackedPartition.accumulate(iter.next()));
                }
            }
        }

        public PartitionIterator makeIterator()
        {
            return new InternalIterator();
        }

        private class InternalIterator extends AbstractIterator<AtomIterator> implements PartitionIterator
        {
            private int i;

            protected AtomIterator computeNext()
            {
                // TODO: we should actually remember if it's a reversed order or not!
                return i < data.size()
                     ? data.get(i++).atomIterator(Slices.ALL, false)
                     : endOfData();
            }

            public void close()
            {
            }
        }
    }

    // TODO
    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithLength(response.isDigestQuery() ? response.digest : ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
            out.writeBoolean(response.isDigestQuery());
            if (!response.isDigestQuery())
                PartitionIterators.serializerForIntraNode().serialize(response.data.makeIterator(), out, version);
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithLength(in);
            boolean isDigest = in.readBoolean();
            assert isDigest == digest.remaining() > 0;

            PartitionIteratorHolder data = null;
            if (!isDigest)
            {
                // This is coming from a remote host
                data = new BufferedPartitionIteratorHolder(PartitionIterators.serializerForIntraNode().deserialize(in, version, LegacyLayout.Flag.FROM_REMOTE));
            }

            return new ReadResponse(data, isDigest ? digest : null);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            // We should call that as this will consume our iterator. We need #8100
            // (or we'll need to buffer before writing until we have it)
            throw new UnsupportedOperationException();

            //TypeSizes typeSizes = TypeSizes.NATIVE;
            //ByteBuffer buffer = response.isDigestQuery() ? response.digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            //int size = typeSizes.sizeof(buffer.remaining());
            //size += buffer.remaining();
            //size += typeSizes.sizeof(response.isDigestQuery());
            //if (!response.isDigestQuery())
            //    size += PartitionIterators.serializerForIntraNode().serializedSize(response.data.makeIterator(), version);
            //return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        out.writeInt(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            Row.serializer.serialize(row, out, version);
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int rowCount = in.readInt();
            //        List<Row> rows = new ArrayList<Row>(rowCount);
            //        for (int i = 0; i < rowCount; i++)
            //            rows.add(Row.serializer.deserialize(in, version));
            //        return new RangeSliceReply(rows);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int size = TypeSizes.NATIVE.sizeof(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            size += Row.serializer.serializedSize(row, version);
            //        return size;
        }
    }
}
