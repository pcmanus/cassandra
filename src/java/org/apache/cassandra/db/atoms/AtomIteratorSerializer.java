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
package org.apache.cassandra.db.atoms;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Serialize/Deserialize an atom iterator.
 *
 * The serialization is composed of a header, follows by the atom of the iterator serialized
 * until we read the end of the partition (see AtomSerializer for details). The header itself
 * is:
 *     <cfid><key><flags><min_timetamp><min_localDelTime><min_ttl>[<static_columns>]<columns>[<partition_deletion>][<static_row>]
 * where:
 *     <cfid> is the table cfid.
 *     <key> is the partition key.
 *     <flags> contains bit flags. Each flag is set if it's corresponding bit is set. From rightmost
 *         bit to leftmost one, the flags are:
 *         - is empty: whether the iterator is empty. If so, nothing follows the <flags>
 *         - is reversed: whether the atoms a serialized in reversed clustering order
 *         - has partition deletion: whether or not there is a <partition_deletion> following
 *         - has static row: whether or not there is a <static_row> following
 *     <min_timestamp> is the base timestamp used for delta-encoding timestamps
 *     <min_localDelTime> is the base localDeletionTime used for delta-encoding local deletion times
 *     <min_ttl> is the base localDeletionTime used for delta-encoding ttls
 *     <static_columns> is the static columns if a static row is present. It's
 *         the number of columns as an unsigned short, followed by the column names.
 *     <columns> is the columns of the rows of the iterator. It's serialized as <static_columns>.
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by AtomSerializer.
 *
 * !!! Please note that the serialized value depends on the schema and as such should not be used as is if
 *     it might be deserialized after the schema as changed !!!
 * TODO: we should add a flag to include the relevant metadata in the header.
 */
public class AtomIteratorSerializer
{
    private static final int IS_EMPTY               = 0x01;
    private static final int IS_REVERSED            = 0x02;
    private static final int HAS_PARTITION_DELETION = 0x04;
    private static final int HAS_STATIC_ROW         = 0x08;

    public static final AtomIteratorSerializer serializer = new AtomIteratorSerializer();

    public void serialize(AtomIterator iterator, DataOutputPlus out, int version)
    throws IOException
    {
        Header header = new Header(iterator.metadata(),
                                   iterator.columns(),
                                   iterator.stats());

        serializeCfId(iterator.metadata().cfId, version);
        iterator.metadata().getKeyValidator().writeValue(iterator.partitionKey().getKey(), out);

        int flags = 0;
        if (iterator.isReverseOrder())
            flags |= IS_REVERSED;

        if (AtomIterators.isEmpty(iterator))
        {
            out.writeByte((byte)(flags | IS_EMPTY));
            return;
        }

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
            flags |= HAS_PARTITION_DELETION;
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            flags |= HAS_STATIC_ROW;

        out.writeByte((byte)flags);

        writeStats(iterator.stats(), out);

        if (hasStatic)
            writeColumns(staticRow.columns(), out);
        writeColumns(iterator.columns().regulars, out);

        if (!partitionDeletion.isLive())
            writeDelTime(partitionDeletion, iterator.stats(), out);

        Header header = new Header(iterator.metadata(),
                                   iterator.columns(),
                                   iterator.stats());

        if (hasStatic)
            AtomSerializer.serializer.serialize(staticRow, header, out, version);

        try (AtomIterato iter = iterator)
        {
            while (iter.hasNext())
                AtomSerializer.serializer.serialize(iter.next(), header, out, version);
            AtomSerializer.writeEndOfPartition(out);
        }
    }

    public AtomIterator deserialize(DataInput in, final int version)
    {
        CFMetaData metadata = deserializeCfId(in, version);
        DecoratedKey key = StorageService.getPartitioner().decorateKey(metadata.getKeyValidator().readValue(in));
        int flags = in.readUnsignedByte();
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
            return AtomIterators.emptyIterator(metadata, key, isReversed);

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;

        AtomStats stats = readStats(in);

        Columns statics = hasStatic ? readColumns(in) : Columns.NONE;
        Columns regulars = readColumns(in);

        final Header header = new Header(metadata,
                                         new PartitionColumns(statics, regulars),
                                         stats);

        DeletionTime partitionDeletion = hasPartitionDeletion ? readDelTime(in, stats) : DeletionTime.LIVE;
        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
        {
            ReusableRow row = new ReusableRow(statics);
            AtomSerializer.serializer.deserialize(in, header, version, row.writer(), null);
            staticRow = row;
        }

        return new AbstractAtomIterator(metadata, key, partitionDeletion, header.columns, staticRow, isReversed, header.stats)
        {
            private final ReusableRow row = new ReusableRow(header.columns.regulars);
            private final ReusableRangeTombstoneMarker marker = new ReusableRangeTombstoneMarker();

            protected Atom computeNext()
            {
                Atom.Kind kind = AtomSerializer.deserialize(in, header, version, row.writer(), marker.writer());
                if (kind == null)
                    return endOfData();

                return kind == Atom.Kind.ROW ? row : marker;
            }
        };
    }

    private void writeColumns(Columns columns, DataOutputPlus out)
    {
        out.writeShort(columns.columnCount());
        for (ColumnDefinition column : columns)
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
    }

    private Columns readColumns(DataInput in)
    {
        int length = in.readUnsignedShort();
        ColumnDefinition[] columns = new ColumnDefinition[length];
        for (int i = 0; i < length; i++)
            columns[i] = ByteBufferUtil.readWithShortLength(in);
        return new Columns(columns);
    }

    private void writeStats(AtomStats stats, DataOutputPlus out)
    {
        out.writeLong(stats.minTimestamp);
        out.writeInt(stats.minLocalDeletionTime);
        out.writeInt(stats.minTTL);
    }

    private AtomStats readStats(DataInput in)
    {
        long minTimestamp = in.readLong();
        int minLocalDeletionTime = in.readInt();
        int minTTL = in.readInt();
        return new AtomStats(minTimestamp, minLocalDeletionTime, minTTL);
    }

    public static void writeDelTime(DeletionTime dt, AtomStats stats, DataOutputPlus out)
    throws IOException
    {
        out.writeLong(dt.markedForDeleteAt() - stats.minTimestamp);
        out.writeInt(dt.localDeletionTime() - stats.minLocalDeletionTime);
    }

    public static DeletionTime readDelTime(DataInput in, AtomStats stats)
    throws IOException
    {
        long markedAt = stats.minTimestamp + in.readLong();
        int localDelTime = stats.minLocalDeletionTime + in.readInt();
        return new SimpleDeletionTime(markedAt, localDelTime);
    }

    public void serializeCfId(UUID cfId, DataOutputPlus out, int version) throws IOException
    {
        UUIDSerializer.serializer.serialize(cfId, out, version);
    }

    public CFMetaData deserializeCfId(DataInput in, int version) throws IOException
    {
        UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
        CFMetaData metadata = Schema.instance.getCF(cfId);
        if (metadata == null)
            throw new UnknownColumnFamilyException("Couldn't find cfId=" + cfId, cfId);

        return metadata;
    }

    public int cfIdSerializedSize(UUID cfId, TypeSizes typeSizes, int version)
    {
        return typeSizes.sizeof(cfId);
    }

    public static class Header
    {
        public final CFMetaData metadata;
        public final PartitionColumns columns;
        public final AtomStats stats;

        private ReusableClusteringPrefix clustering;

        private Header(CFMetaData metadata, PartitionColumns columns, AtomStats stats)
        {
            this.metadata = metadata;
            this.columns = columns;
            this.stats = stats;
        }

        public ReusableClusteringPrefix reusableClustering()
        {
            if (clustering == null)
                clustering = new ReusableClusteringPrefix(metadata.clusteringColumns().size());
            return clustering;
        }
    }
}
