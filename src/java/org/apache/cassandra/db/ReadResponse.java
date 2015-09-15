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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    // Serializer for read responses (should only be used for actual serialization, not deserialization)
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();

    // Deserializer for digest responses
    public static final IVersionedSerializer<ReadResponse> digestDeserializer = new DigestDeserializer();

    // Serializer for the pre-3.0 responses.
    private static final IVersionedSerializer<ReadResponse> legacySinglePartitionSerializer = new LegacySinglePartitionSerializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    public static ReadResponse createDataResponse(ReadCommand command, UnfilteredPartitionIterator data)
    {
        return new DataResponse(command, data);
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, int version)
    {
        return new DigestResponse(makeDigest(data, version));
    }

    public abstract UnfilteredPartitionIterator makeIterator();
    public abstract ByteBuffer digest();

    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, int version)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest, version);
        return ByteBuffer.wrap(digest.digest());
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

        public ByteBuffer digest()
        {
            return digest;
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    private static abstract class AbstractDataResponse extends ReadResponse
    {
        protected final ReadCommand command;

        protected AbstractDataResponse(ReadCommand command)
        {
            this.command = command;
        }

        public ByteBuffer digest()
        {
            try (UnfilteredPartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator, command.digestVersion());
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    /*
     * A response for a local read. Such response is either used directly, or serialized to be sent to a remote node.
     */
    private static class DataResponse extends AbstractDataResponse
    {
        private final List<ImmutableBTreePartition> partitions;

        protected DataResponse(ReadCommand command, UnfilteredPartitionIterator response)
        {
            super(command);
            this.partitions = build(command, response);
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

                public boolean isForThrift()
                {
                    return command.isForThrift();
                }

                public CFMetaData metadata()
                {
                    return command.metadata();
                }

                public boolean hasNext()
                {
                    return idx < partitions.size();
                }

                public UnfilteredRowIterator next()
                {
                    // TODO: we know rows don't require any filtering and that we return everything. We ought to be able to optimize this.
                    return partitions.get(idx++).unfilteredIterator(command.columnFilter(), Slices.ALL, command.isReversed());
                }
            };
        }

        private static List<ImmutableBTreePartition> build(ReadCommand command, UnfilteredPartitionIterator iterator)
        {
            if (!iterator.hasNext())
                return Collections.emptyList();

            if (command instanceof SinglePartitionReadCommand)
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    List<ImmutableBTreePartition> partitions = Collections.singletonList(ImmutableBTreePartition.create(partition));
                    assert !iterator.hasNext();
                    return partitions;
                }
            }

            List<ImmutableBTreePartition> partitions = new ArrayList<>();
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    partitions.add(ImmutableBTreePartition.create(partition));
                }
            }
            return partitions;
        }
    }

    private static class RemoteDataResponse extends DataResponse
    {
        private RemoteDataResponse(ReadCommand command, UnfilteredPartitionIterator response)
        {
            super(command, response);
        }
    }

    /**
     * A remote response from a pre-3.0 node.  This needs a separate class in order to cleanly handle trimming and
     * reversal of results when the read command calls for it.  Pre-3.0 nodes always return results in the normal
     * sorted order, even if the query asks for reversed results.  Additionally,  pre-3.0 nodes do not have a notion of
     * exclusive slices on non-composite tables, so extra rows may need to be trimmed.
     */
    @VisibleForTesting
    static class LegacyRemoteDataResponse extends AbstractDataResponse
    {
        private final List<ImmutableBTreePartition> partitions;

        @VisibleForTesting
        LegacyRemoteDataResponse(ReadCommand command, List<ImmutableBTreePartition> partitions)
        {
            super(command);
            this.partitions = partitions;
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            // Due to a bug in the serialization of AbstractBounds, anything that isn't a Range is understood by pre-3.0 nodes
            // as a Bound, which means IncludingExcludingBounds and ExcludingBounds responses may include keys they shouldn't.
            // So filter partitions that shouldn't be included here.
            boolean skipFirst = false;
            boolean skipLast = false;
            if (!partitions.isEmpty() && command instanceof PartitionRangeReadCommand)
            {
                AbstractBounds<PartitionPosition> keyRange = ((PartitionRangeReadCommand)command).dataRange().keyRange();
                boolean isExcludingBounds = keyRange instanceof ExcludingBounds;
                skipFirst = isExcludingBounds && !keyRange.contains(partitions.get(0).partitionKey());
                skipLast = (isExcludingBounds || keyRange instanceof IncludingExcludingBounds) && !keyRange.contains(partitions.get(partitions.size() - 1).partitionKey());
            }

            final List<ImmutableBTreePartition> toReturn;
            if (skipFirst || skipLast)
            {
                toReturn = partitions.size() == 1
                         ? Collections.emptyList()
                         : partitions.subList(skipFirst ? 1 : 0, skipLast ? partitions.size() - 1 : partitions.size());
            }
            else
            {
                toReturn = partitions;
            }

            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

                public boolean isForThrift()
                {
                    return command.isForThrift();
                }

                public CFMetaData metadata()
                {
                    return command.metadata();
                }

                public boolean hasNext()
                {
                    return idx < toReturn.size();
                }

                public UnfilteredRowIterator next()
                {
                    ImmutableBTreePartition partition = toReturn.get(idx++);


                    ClusteringIndexFilter filter = command.clusteringIndexFilter(partition.partitionKey());

                    // Pre-3.0, we didn't have a way to express exclusivity for non-composite comparators, so all slices were
                    // inclusive on both ends. If we have exclusive slice ends, we need to filter the results here.
                    if (!command.metadata().isCompound())
                        return filter.filter(partition.sliceableUnfilteredIterator(command.columnFilter(), filter.isReversed()));

                    return partition.unfilteredIterator(command.columnFilter(), Slices.ALL, filter.isReversed());
                }
            };
        }
    }

    /**
     * Only for _serializing_ a ReadResponse. The deserialization method throws and Deserializer should be used instead.
     */
    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                legacySinglePartitionSerializer.serialize(response, out, version);
                return;
            }

            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                assert response instanceof DataResponse;
                DataResponse dataResponse = (DataResponse)response;
                try (UnfilteredPartitionIterator iter = dataResponse.makeIterator())
                {
                    UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, dataResponse.command.columnFilter(), out, version);
                }
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(ReadResponse response, int version)
        {
            if (version < MessagingService.VERSION_30)
                return legacySinglePartitionSerializer.serializedSize(response, version);

            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                assert response instanceof DataResponse;
                DataResponse dataResponse = (DataResponse)response;
                try (UnfilteredPartitionIterator iter = dataResponse.makeIterator())
                {
                    size += UnfilteredPartitionIterators.serializerForIntraNode().serializedSize(iter, dataResponse.command.columnFilter(), version);
                }
            }
            return size;
        }
    }

    /**
     * Only for _deserializing_ a digest ReadResponse. The serialization methods throw and Serializer should be used instead.
     */
    private static class DigestDeserializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                int digestSize = in.readInt();
                assert digestSize > 0;
                byte[] digest = new byte[digestSize];
                in.readFully(digest, 0, digestSize);
                boolean isDigest = in.readBoolean();
                assert isDigest;
                return new DigestResponse(ByteBuffer.wrap(digest));
            }

            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            assert digest.hasRemaining();
            return new DigestResponse(digest);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Only for _deserializing_ a data ReadResponse. The serialization methods throw and Serializer should be used instead.
     */
    public static class Deserializer implements IVersionedSerializer<ReadResponse>
    {
        private final ReadCommand command;

        public Deserializer(ReadCommand command)
        {
            this.command = command;
        }

        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                return command instanceof PartitionRangeReadCommand
                     ? LegacyRangeSliceReplySerializer.deserialize(in, version, command)
                     : LegacySinglePartitionSerializer.deserialize(in, version, command);

            // We could use skipWithVIntLength below, but we expect an empty value and readWithVIntLength don't
            // allocate in that case, so using it allows to validate our assumption.
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            assert !digest.hasRemaining();

            return new RemoteDataResponse(command,
                                          UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, version, command.metadata(), command.columnFilter(), SerializationHelper.Flag.FROM_REMOTE));
        }

        public long serializedSize(ReadResponse response, int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class LegacySinglePartitionSerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            out.writeInt(digest.remaining());
            out.write(digest);
            out.writeBoolean(isDigest);
            if (!isDigest)
            {
                assert response instanceof DataResponse;
                DataResponse dataResponse = (DataResponse)response;
                assert dataResponse.partitions.size() == 1; // We should be going through LegacyRangeSliceReplySerializer otherwise
                try (UnfilteredRowIterator partition = dataResponse.partitions.get(0).unfilteredIterator())
                {
                    ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                    LegacyLayout.serializeAsLegacyPartition(partition, out, version);
                }
            }
            return;
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public static ReadResponse deserialize(DataInputPlus in, int version, ReadCommand command) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            int digestSize = in.readInt();
            assert digestSize == 0; // This is handled by DigestDeserializer
            boolean isDigest = in.readBoolean();
            assert !isDigest;

            // ReadResponses from older versions are always single-partition (ranges are handled by RangeSliceReply)
            ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
            UnfilteredRowIterator rowIterator = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key);
            if (rowIterator == null)
                return new LegacyRemoteDataResponse(command, Collections.emptyList());

            try
            {
                return new LegacyRemoteDataResponse(command, Collections.singletonList(ImmutableBTreePartition.create(rowIterator)));
            }
            finally
            {
                rowIterator.close();
            }
        }

        public long serializedSize(ReadResponse response, int version)
        {
            assert version < MessagingService.VERSION_30;

            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            long size = TypeSizes.sizeof(digest.remaining())
                      + digest.remaining()
                      + TypeSizes.sizeof(isDigest);
            if (!isDigest)
            {
                assert response instanceof DataResponse;
                DataResponse dataResponse = (DataResponse)response;
                assert dataResponse.partitions.size() == 1; // We should be going through LegacyRangeSliceReplySerializer otherwise
                try (UnfilteredRowIterator partition = dataResponse.partitions.get(0).unfilteredIterator())
                {
                    size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                    size += LegacyLayout.serializedSizeAsLegacyPartition(partition, version);
                }
            }
            return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            // determine the number of partitions upfront for serialization
            int numPartitions = 0;
            assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator atomIterator = iterator.next())
                    {
                        numPartitions++;

                        // we have to fully exhaust the subiterator
                        while (atomIterator.hasNext())
                            atomIterator.next();
                    }
                }
            }

            out.writeInt(numPartitions);

            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                        LegacyLayout.serializeAsLegacyPartition(partition, out, version);
                    }
                }
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public static ReadResponse deserialize(DataInputPlus in, int version, ReadCommand command) throws IOException
        {
            int partitionCount = in.readInt();
            ArrayList<ImmutableBTreePartition> partitions = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                try (UnfilteredRowIterator partition = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key))
                {
                    partitions.add(ImmutableBTreePartition.create(partition));
                }
            }
            return new LegacyRemoteDataResponse(command, partitions);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            assert version < MessagingService.VERSION_30;
            long size = TypeSizes.sizeof(0);  // number of partitions

            assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                        size += LegacyLayout.serializedSizeAsLegacyPartition(partition, version);
                    }
                }
            }
            return size;
        }
    }
}
