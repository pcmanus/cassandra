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
package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.io.IOError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Serialize/Deserialize an unfiltered row iterator.
 *
 * The serialization is composed of a header, follows by the rows and range tombstones of the iterator serialized
 * until we read the end of the partition (see UnfilteredSerializer for details). The header itself
 * is:
 *     <flags><key>[<partition_deletion>][<static_row>][<row_estimate>]
 * where:
 *     <flags> contains bit flags. Each flag is set if it's corresponding bit is set. The description of each
 *             can be find on the constants below.
 *     <key> is the partition key.
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by UnfilteredSerializer.
 *     <row_estimate> is the (potentially estimated) number of rows serialized. This is only used for
 *         the purpose of sizing on the receiving end and should not be relied upon too strongly.
 *
 * Please note that the format described above is the on-wire format. On-disk, the format is basically the
 * same, but the header is written once per sstable, not once per-partition. Further, the actual row and
 * range tombstones are not written using this class, but rather by {@link ColumnIndex}.
 */
public class UnfilteredRowIteratorSerializer
{
    protected static final Logger logger = LoggerFactory.getLogger(UnfilteredRowIteratorSerializer.class);

    private static final int END_OF_MESSAGE         = 0x01; // Indicates the end of a list of partitions. Only used when encoding multiple partitions
                                                            // on wire to detect the end. Shouldn't be set otherwise.
    private static final int IS_EMPTY               = 0x02; // Whether the encoded partition is empty. If so, nothing follows the <key> field.
    private static final int IS_REVERSED            = 0x04; // Whether the encoded partition is in reverse clustering order or not.
    private static final int HAS_PARTITION_DELETION = 0x08; // Whether the encoded partition has a (non-live) partition level deletion.
    private static final int HAS_STATIC_ROW         = 0x10; // Whether the encoded partition has a (non-empty) static row.
    private static final int HAS_ROW_ESTIMATE       = 0x20; // Indicates the presence of estimate for the number of rows contained in the encoded partition.

    public static final UnfilteredRowIteratorSerializer serializer = new UnfilteredRowIteratorSerializer();

    // Should only be used for the on-wire format.
    public void serialize(UnfilteredRowIterator iterator, DataOutputPlus out, int version, SerializationHeader header) throws IOException
    {
        serialize(iterator, out, version, header, -1);
    }

    // Should only be used for the on-wire format.
    public void serialize(UnfilteredRowIterator iterator, DataOutputPlus out, int version, SerializationHeader header, int rowEstimate) throws IOException
    {
        int flags = 0;
        if (iterator.isReverseOrder())
            flags |= IS_REVERSED;

        if (iterator.isEmpty())
        {
            out.writeByte((byte)(flags | IS_EMPTY));
            header.keyType().writeValue(iterator.partitionKey().getKey(), out);
            return;
        }

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
            flags |= HAS_PARTITION_DELETION;
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            flags |= HAS_STATIC_ROW;

        if (rowEstimate >= 0)
            flags |= HAS_ROW_ESTIMATE;

        out.writeByte((byte)flags);
        header.keyType().writeValue(iterator.partitionKey().getKey(), out);

        if (!partitionDeletion.isLive())
            header.writeDeletionTime(partitionDeletion, out);

        if (hasStatic)
            UnfilteredSerializer.serializer.serialize(staticRow, header, out, version);

        if (rowEstimate >= 0)
            out.writeVInt(rowEstimate);

        while (iterator.hasNext())
            UnfilteredSerializer.serializer.serialize(iterator.next(), header, out, version);
        UnfilteredSerializer.serializer.writeEndOfPartition(out);
    }

    public void writeEndOfMessage(DataOutputPlus out)
    {
        out.writeByte(END_OF_MESSAGE);
    }

    // Please note that this consume the iterator, and as such should not be called unless we have a simple way to
    // recreate an iterator for both serialize and serializedSize, which is mostly only PartitionUpdate/ArrayBackedCachedPartition.
    public long serializedSize(UnfilteredRowIterator iterator,  int version, SerializationHeader header, int rowEstimate)
    {
        assert rowEstimate >= 0;

        long size = 1 // flags
                  + header.keyType().writtenLength(iterator.partitionKey().getKey());

        if (iterator.isEmpty())
            return size;

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        if (!partitionDeletion.isLive())
            size += header.deletionTimeSerializedSize(partitionDeletion);

        if (hasStatic)
            size += UnfilteredSerializer.serializer.serializedSize(staticRow, header, version);

        if (rowEstimate >= 0)
            size += TypeSizes.sizeofVInt(rowEstimate);

        while (iterator.hasNext())
            size += UnfilteredSerializer.serializer.serializedSize(iterator.next(), header, version);
        size += UnfilteredSerializer.serializer.serializedSizeEndOfPartition();

        return size;
    }

    public Header deserializeHeader(DataInputPlus in, int version, CFMetaData metadata, SerializationHelper.Flag flag, SerializationHeader header) throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & END_OF_MESSAGE) != 0)
            return null;

        DecoratedKey key = StorageService.getPartitioner().decorateKey(header.keyType().readValue(in));
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
            return new Header(key, isReversed, true, null, null, 0);

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;
        boolean hasRowEstimate = (flags & HAS_ROW_ESTIMATE) != 0;

        DeletionTime partitionDeletion = hasPartitionDeletion ? header.readDeletionTime(in) : DeletionTime.LIVE;

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            staticRow = UnfilteredSerializer.serializer.deserializeStaticRow(in, header, new SerializationHelper(header.metadata(), version, flag));

        int rowEstimate = hasRowEstimate ? (int)in.readVInt() : -1;
        return new Header(key, isReversed, false, partitionDeletion, staticRow, rowEstimate);
    }

    public UnfilteredRowIterator deserialize(DataInputPlus in, int version, CFMetaData metadata, SerializationHelper.Flag flag, SerializationHeader header, Header partitionHeader) throws IOException
    {
        assert header != null;

        CFMetaData metadata = header.metadata();
        if (partitionHeader.isEmpty)
            return UnfilteredRowIterators.emptyIterator(metadata, partitionHeader.key, partitionHeader.isReversed);

        final SerializationHelper helper = new SerializationHelper(metadata, version, flag);
        return new AbstractUnfilteredRowIterator(metadata, partitionHeader.key, partitionHeader.partitionDeletion, header.columns(), partitionHeader.staticRow, partitionHeader.isReversed, header.stats())
        {
            private final Row.Builder builder = ArrayBackedRow.sortedBuilder(header.columns().regulars);

            protected Unfiltered computeNext()
            {
                try
                {
                    Unfiltered unfiltered = UnfilteredSerializer.serializer.deserialize(in, header, helper, builder);
                    return unfiltered == null ? endOfData() : unfiltered;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public UnfilteredRowIterator deserialize(DataInputPlus in, int version, CFMetaData metadata, SerializationHelper.Flag flag, SerializationHeader header) throws IOException
    {
        Header partitionHeader = deserializeHeader(in, version, flag, header);
        if (partitionHeader == null)
            return null;

        return deserialize(in, version, flag, header, partitionHeader);
    }

    public static class Header
    {
        public final DecoratedKey key;
        public final boolean isReversed;
        public final boolean isEmpty;
        public final DeletionTime partitionDeletion;
        public final Row staticRow;
        public final int rowEstimate; // -1 if no estimate

        private Header(DecoratedKey key,
                       boolean isReversed,
                       boolean isEmpty,
                       DeletionTime partitionDeletion,
                       Row staticRow,
                       int rowEstimate)
        {
            this.key = key;
            this.isReversed = isReversed;
            this.isEmpty = isEmpty;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.rowEstimate = rowEstimate;
        }

        @Override
        public String toString()
        {
            return String.format("{key=%s, isReversed=%b, isEmpty=%b, del=%s, staticRow=%s, rowEstimate=%d}",
                                   key, isReversed, isEmpty, partitionDeletion, staticRow, rowEstimate);
        }
    }
}
