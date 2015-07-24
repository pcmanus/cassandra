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

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data)
    {
        return new DataResponse(data);
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, int version)
    {
        return new DigestResponse(makeDigest(data, version));
    }

    public abstract UnfilteredPartitionIterator makeIterator();
    public abstract ByteBuffer digest(int version);
    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, int version)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest, version);
        return ByteBuffer.wrap(digest.digest());
    }

    public void preprocessLegacyResults(ReadCommand command)
    {
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(int version)
        {
            // We assume that the digest is in the proper version, which bug excluded should be true since this is called with
            // ReadCommand.digestVersion() as argument and that's also what we use to produce the digest in the first place.
            // Validating it's the proper digest in this method would require sending back the digest version along with the
            // digest which would wast bandwith for little gain.
            return digest;
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    private static class DataResponse extends ReadResponse
    {
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final SerializationHelper.Flag flag;

        private DataResponse(ByteBuffer data)
        {
            this.data = data;
            this.flag = SerializationHelper.Flag.FROM_REMOTE;
        }

        private DataResponse(UnfilteredPartitionIterator iter)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
                this.flag = SerializationHelper.Flag.LOCAL;
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            try
            {
                DataInputPlus in = new DataInputBuffer(data, true);
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest(int version)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator, version);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    /**
     * A remote response from a pre-3.0 node.  This needs a separate class in order to cleanly handle trimming and
     * reversal of results when the read command calls for it.  Pre-3.0 nodes always return results in the normal
     * sorted order, even if the query asks for reversed results.  Additionally,  pre-3.0 nodes do not have a notion of
     * exclusive slices on non-composite tables, so extra rows may need to be trimmed.
     */
    private static class LegacyRemoteDataResponse extends ReadResponse
    {
        private UnfilteredPartitionIterator iterator;
        private ByteBuffer data;

        private LegacyRemoteDataResponse(UnfilteredPartitionIterator iterator)
        {
            this.iterator = iterator;
            this.data = null;
        }

        private void buildDataBuffer()
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            try
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iterator, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            if (data == null)
                buildDataBuffer();

            try
            {
                DataInputPlus in = new DataInputPlus.DataInputStreamPlus(ByteBufferUtil.inputStream(data));
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, SerializationHelper.Flag.FROM_REMOTE);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        @Override
        public void preprocessLegacyResults(ReadCommand command)
        {
            assert data == null : "Legacy results should have been processed before the data was used";

            boolean needsReversal = command.rowsInPartitionAreReversed();

            // Pre-3.0, we didn't have a way to express exclusivity for non-composite comparators, so all slices were
            // inclusive on both ends.  If we have exclusive slice ends, we need to filter the results here.
            boolean needsTrimming = !command.metadata().isCompound();

            if (!needsReversal && !needsTrimming)
                return;

            final UnfilteredPartitionIterator originalIterator = iterator;

            iterator = new UnfilteredPartitionIterator()
            {
                UnfilteredRowIterator next;

                @Override
                public boolean isForThrift()
                {
                    return originalIterator.isForThrift();
                }

                @Override
                public boolean hasNext()
                {
                    return originalIterator.hasNext();
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    next = originalIterator.next();

                    boolean stillNeedToReverse = needsReversal;
                    if (needsTrimming)
                    {
                        ClusteringIndexFilter filter = command.clusteringIndexFilter(next.partitionKey());
                        if (filter.kind() == ClusteringIndexFilter.Kind.SLICE && !filter.selectsAllPartition())
                        {
                            // handle reversal here if we need to do it anyway
                            boolean reversed = filter.isReversed() || needsReversal;
                            stillNeedToReverse = false;

                            ArrayBackedPartition partition = ArrayBackedPartition.create(next);
                            next = partition.unfilteredIterator(
                                    command.columnFilter(), ((ClusteringIndexSliceFilter) filter).requestedSlices(), reversed);
                        }
                    }

                    if (stillNeedToReverse)
                        next = ArrayBackedPartition.create(next).unfilteredIterator(command.columnFilter(), Slices.ALL, true);

                    return next;
                }

                @Override
                public void close()
                {
                    try
                    {
                        originalIterator.close();
                    }
                    finally
                    {
                        if (next != null)
                            next.close();
                    }
                }
            };
        }

        public ByteBuffer digest(int version)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator, version);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            boolean isDigest = response instanceof DigestResponse;
            if (version < MessagingService.VERSION_30)
            {
                ByteBuffer buffer = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
                out.writeInt(buffer.remaining());
                out.write(buffer);
                out.writeBoolean(isDigest);
                if (!isDigest)
                    UnfilteredPartitionIterators.serializerForIntraNode().serialize(response.makeIterator(), out, version);
                return;
            }

            ByteBufferUtil.writeWithShortLength(isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                byte[] digest = null;
                int digestSize = in.readInt();
                if (digestSize > 0)
                {
                    digest = new byte[digestSize];
                    in.readFully(digest, 0, digestSize);
                }
                boolean isDigest = in.readBoolean();
                assert isDigest == digestSize > 0;
                if (isDigest)
                {
                    assert digest != null;
                    return new DigestResponse(ByteBuffer.wrap(digest));
                }

                // ReadResponses from older versions are always single-partition (ranges are handled by RangeSliceReply)
                DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));

                UnfilteredRowIterator rowIterator;
                boolean present = in.readBoolean();
                if (!present)
                    return new LegacyRemoteDataResponse(UnfilteredPartitionIterators.EMPTY);

                rowIterator = UnfilteredPartitionIterators.serializerForIntraNode().deserializeLegacyPartition(in, key, version);
                UnfilteredPartitionIterator iterator = new UnfilteredPartitionIterators.SingletonPartitionIterator(rowIterator, true);
                return new LegacyRemoteDataResponse(iterator);
            }

            ByteBuffer digest = ByteBufferUtil.readWithShortLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            ByteBuffer data = ByteBufferUtil.readWithLength(in);
            return new DataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            boolean isDigest = response instanceof DigestResponse;
            long size = 0;

            if (version < MessagingService.VERSION_30)
            {
                ByteBuffer buffer = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
                size += TypeSizes.sizeof(buffer.remaining());
                size += buffer.remaining();
                size += TypeSizes.sizeof(isDigest);
                if (!isDigest)
                    size += UnfilteredPartitionIterators.serializerForIntraNode().serializedSize(response.makeIterator(), version);
                return size;
            }

            size += ByteBufferUtil.serializedSizeWithShortLength(isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithLength(data);
            }
            return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            // determine the number of partitions upfront for serialization
            int numPartitions = 0;
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator atomIterator = iterator.next())
                    {
                        numPartitions++;

                        // we have to fully exhaust the subiterator
                        while(atomIterator.hasNext())
                            atomIterator.next();
                    }
                }
            }
            out.writeInt(numPartitions);
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iterator, out, version);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            UnfilteredPartitionIterator iterator = UnfilteredPartitionIterators.serializerForIntraNode().deserialize(
                    in, version, SerializationHelper.Flag.FROM_REMOTE);
            return new LegacyRemoteDataResponse(iterator);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            int size = TypeSizes.sizeof(0);  // number of partitions
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                size += UnfilteredPartitionIterators.serializerForIntraNode().serializedSize(iterator, version);
            }
            return size;
        }
    }
}
