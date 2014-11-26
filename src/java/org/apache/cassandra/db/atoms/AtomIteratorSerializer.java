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
import java.io.IOError;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
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
 *         - has row estimate: whether or not there is a <row_estimate> following
 *     <min_timestamp> is the base timestamp used for delta-encoding timestamps
 *     <min_localDelTime> is the base localDeletionTime used for delta-encoding local deletion times
 *     <min_ttl> is the base localDeletionTime used for delta-encoding ttls
 *     <static_columns> is the static columns if a static row is present. It's
 *         the number of columns as an unsigned short, followed by the column names.
 *     <columns> is the columns of the rows of the iterator. It's serialized as <static_columns>.
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by AtomSerializer.
 *     <row_estimate> is the (potentially estimated) number of rows serialized. This is only use for
 *         the purpose of some sizing on the receiving end and should not be relied upon too strongly.
 *
 * !!! Please note that the serialized value depends on the schema and as such should not be used as is if
 *     it might be deserialized after the schema as changed !!!
 * TODO: we should add a flag to include the relevant metadata in the header for commit log etc.....
 */
public class AtomIteratorSerializer
{
    private static final int IS_EMPTY               = 0x01;
    private static final int IS_REVERSED            = 0x02;
    private static final int HAS_PARTITION_DELETION = 0x04;
    private static final int HAS_STATIC_ROW         = 0x08;
    private static final int HAS_ROW_ESTIMATE       = 0x10;

    public static final AtomIteratorSerializer serializer = new AtomIteratorSerializer();

    public void serialize(AtomIterator iterator, DataOutputPlus out, int version) throws IOException
    {
        serialize(iterator, out, version, -1);
    }

    public void serialize(AtomIterator iterator, DataOutputPlus out, int version, int rowEstimate) throws IOException
    {
        Header header = new Header(iterator.metadata(),
                                   iterator.partitionKey(),
                                   iterator.columns(),
                                   iterator.stats(),
                                   iterator.isReverseOrder());

        serializeCfId(iterator.metadata().cfId, out, version);
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

        if (rowEstimate >= 0)
            flags |= HAS_ROW_ESTIMATE;

        out.writeByte((byte)flags);

        writeStats(iterator.stats(), out);

        if (hasStatic)
            writeColumns(staticRow.columns(), out);
        writeColumns(iterator.columns().regulars, out);

        if (!partitionDeletion.isLive())
            writeDelTime(partitionDeletion, header, out);

        if (hasStatic)
            AtomSerializer.serializer.serialize(staticRow, header, out, version);

        if (rowEstimate >= 0)
            out.writeInt(rowEstimate);

        try (AtomIterator iter = iterator)
        {
            while (iter.hasNext())
                AtomSerializer.serializer.serialize(iter.next(), header, out, version);
            AtomSerializer.serializer.writeEndOfPartition(out);
        }
    }

    // Please not that this consume the iterator, and as such should not be called unless we have a simple way to
    // recreate an iterator for both serialize and serializedSize, which is mostly only PartitionUpdate
    public long serializedSize(AtomIterator iterator, int version, int rowEstimate)
    {
        assert rowEstimate >= 0;

        Header header = new Header(iterator.metadata(),
                                   iterator.partitionKey(),
                                   iterator.columns(),
                                   iterator.stats(),
                                   iterator.isReverseOrder());

        // TODO: we should use vint!
        TypeSizes sizes = TypeSizes.NATIVE;

        long size = cfIdSerializedSize(iterator.metadata().cfId, sizes, version)
                  + iterator.metadata().getKeyValidator().writtenLength(iterator.partitionKey().getKey(), sizes)
                  + 1; // flags

        if (AtomIterators.isEmpty(iterator))
            return size;

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        size += statsSerializedSize(iterator.stats(), sizes);
        if (hasStatic)
            size += columnsSerializedSize(staticRow.columns(), sizes);

        size += columnsSerializedSize(iterator.columns().regulars, sizes);

        if (!partitionDeletion.isLive())
            size += delTimeSerializedSize(partitionDeletion, header, sizes);

        if (hasStatic)
            size += AtomSerializer.serializer.serializedSize(staticRow, header, version, sizes);

        size += sizes.sizeof(rowEstimate);

        try (AtomIterator iter = iterator)
        {
            while (iter.hasNext())
                size += AtomSerializer.serializer.serializedSize(iter.next(), header, version, sizes);
            size += AtomSerializer.serializer.serializedSizeEndOfPartition(sizes);
        }
        catch (IOException e)
        {
            // Since this method is supposed to be only called for entirely in-memory data, we shouldn't
            // get an IOException while closing.
            throw new RuntimeException();
        }

        return size;
    }

    public FullHeader deserializeHeader(DataInput in, int version) throws IOException
    {
        CFMetaData metadata = deserializeCfId(in, version);
        DecoratedKey key = StorageService.getPartitioner().decorateKey(metadata.getKeyValidator().readValue(in));
        int flags = in.readUnsignedByte();
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
            return new FullHeader(new Header(metadata, key, PartitionColumns.NONE, AtomStats.NO_STATS, isReversed), true, null, null, 0);

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;
        boolean hasRowEstimate = (flags & HAS_ROW_ESTIMATE) != 0;

        AtomStats stats = readStats(in);

        Columns statics = hasStatic ? readColumns(in, metadata) : Columns.NONE;
        Columns regulars = readColumns(in, metadata);

        Header header = new Header(metadata, key, new PartitionColumns(statics, regulars), stats, isReversed);

        DeletionTime partitionDeletion = hasPartitionDeletion ? readDelTime(in, header) : DeletionTime.LIVE;

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
        {
            ReusableRow row = new ReusableRow(statics);
            AtomSerializer.serializer.deserialize(in, header, version, row.writer(), null);
            staticRow = row;
        }
        int rowEstimate = hasRowEstimate ? in.readInt() : -1;
        return new FullHeader(header, false, partitionDeletion, staticRow, rowEstimate);
    }

    public void deserializeAtoms(DataInput in, int version, Header header, Row.Writer rowWriter, RangeTombstoneMarker.Writer markerWriter) throws IOException
    {
        while (AtomSerializer.serializer.deserialize(in, header, version, rowWriter, markerWriter) != null);
    }

    public AtomIterator deserialize(final DataInput in, final int version) throws IOException
    {
        FullHeader fh = deserializeHeader(in, version);
        final Header h = fh.header;

        if (fh.isEmpty)
            return AtomIterators.emptyIterator(h.metadata, h.key, h.isReversed);

        return new AbstractAtomIterator(h.metadata, h.key, fh.partitionDeletion, h.columns, fh.staticRow, h.isReversed, h.stats)
        {
            private final ReusableRow row = new ReusableRow(h.columns.regulars);
            private final ReusableRangeTombstoneMarker marker = new ReusableRangeTombstoneMarker();

            protected Atom computeNext()
            {
                try
                {
                    Atom.Kind kind = AtomSerializer.serializer.deserialize(in, h, version, row.writer(), marker.writer());
                    if (kind == null)
                        return endOfData();

                    return kind == Atom.Kind.ROW ? row : marker;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    private void writeColumns(Columns columns, DataOutputPlus out) throws IOException
    {
        out.writeShort(columns.columnCount());
        for (ColumnDefinition column : columns)
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
    }

    private long columnsSerializedSize(Columns columns, TypeSizes sizes)
    {
        long size = sizes.sizeof((short)columns.columnCount());
        for (ColumnDefinition column : columns)
            size += sizes.sizeofWithShortLength(column.name.bytes);
        return size;
    }

    private Columns readColumns(DataInput in, CFMetaData metadata) throws IOException
    {
        int length = in.readUnsignedShort();
        ColumnDefinition[] columns = new ColumnDefinition[length];
        for (int i = 0; i < length; i++)
        {
            ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
            ColumnDefinition column = metadata.getColumnDefinition(name);
            if (column == null)
                throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
            columns[i] = column;
        }
        return new Columns(columns);
    }

    private void writeStats(AtomStats stats, DataOutputPlus out) throws IOException
    {
        out.writeLong(stats.minTimestamp);
        out.writeInt(stats.minLocalDeletionTime);
        out.writeInt(stats.minTTL);
    }

    private long statsSerializedSize(AtomStats stats, TypeSizes sizes)
    {
        return sizes.sizeof(stats.minTimestamp)
             + sizes.sizeof(stats.minLocalDeletionTime)
             + sizes.sizeof(stats.minTTL);
    }

    private AtomStats readStats(DataInput in) throws IOException
    {
        long minTimestamp = in.readLong();
        int minLocalDeletionTime = in.readInt();
        int minTTL = in.readInt();
        return new AtomStats(minTimestamp, minLocalDeletionTime, minTTL);
    }

    public static void writeDelTime(DeletionTime dt, Header header, DataOutputPlus out) throws IOException
    {
        out.writeLong(header.encodeTimestamp(dt.markedForDeleteAt()));
        out.writeInt(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static long delTimeSerializedSize(DeletionTime dt, Header header, TypeSizes sizes)
    {
        return sizes.sizeof(header.encodeTimestamp(dt.markedForDeleteAt()))
             + sizes.sizeof(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static DeletionTime readDelTime(DataInput in, Header header) throws IOException
    {
        long markedAt = header.decodeTimestamp(in.readLong());
        int localDelTime = header.decodeDeletionTime(in.readInt());
        return new SimpleDeletionTime(markedAt, localDelTime);
    }

    public void serializeCfId(UUID cfId, DataOutputPlus out, int version) throws IOException
    {
        UUIDSerializer.serializer.serialize(cfId, out, version);
    }

    public CFMetaData deserializeCfId(DataInput in, int version) throws IOException
    {
        UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
        CFMetaData metadata = Schema.instance.getCFMetaData(cfId);
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
        private static final int DEFAULT_BASE_DELETION = computeDefaultBaseDeletion();

        public final CFMetaData metadata;
        public final DecoratedKey key;
        public final PartitionColumns columns;
        public final AtomStats stats;
        public final boolean isReversed;

        private final long baseTimestamp;
        private final int baseDeletionTime;
        private final int baseTTL;

        private ReusableClusteringPrefix clustering;

        private Header(CFMetaData metadata,
                       DecoratedKey key,
                       PartitionColumns columns,
                       AtomStats stats,
                       boolean isReversed)
        {
            this.metadata = metadata;
            this.key = key;
            this.columns = columns;
            this.stats = stats;
            this.isReversed = isReversed;

            // Not that if a given stats is unset, it means that either it's unused (there is
            // no tombstone whatsoever for instance) or that we have no information on it. In
            // that former case, it doesn't matter which base we use but in the former, we use
            // bases that are more likely to provide small encoded values than the default
            // "unset" value.
            this.baseTimestamp = stats.minTimestamp == Cells.NO_TIMESTAMP ? 0 : stats.minTimestamp;
            this.baseDeletionTime = stats.minLocalDeletionTime == Cells.NO_DELETION_TIME ? DEFAULT_BASE_DELETION : stats.minLocalDeletionTime;
            this.baseTTL = stats.minTTL;
        }

        private static int computeDefaultBaseDeletion()
        {
            // We need a fixed default, but one that is likely to provide small values (close to 0) when
            // substracted to deletion times. Since deletion times are 'the current time in seconds', we
            // use as base Jan 1, 2015 (in seconds).
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
            c.set(Calendar.YEAR, 2015);
            c.set(Calendar.MONTH, Calendar.JANUARY);
            c.set(Calendar.DAY_OF_MONTH, 1);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            return (int)(c.getTimeInMillis() / 1000);
        }

        public ReusableClusteringPrefix reusableClustering()
        {
            if (clustering == null)
                clustering = new ReusableClusteringPrefix(metadata.clusteringColumns().size());
            return clustering;
        }

        public long encodeTimestamp(long timestamp)
        {
            return timestamp - baseTimestamp;
        }

        public long decodeTimestamp(long timestamp)
        {
            return baseTimestamp + timestamp;
        }

        public int encodeDeletionTime(int deletionTime)
        {
            return deletionTime - baseDeletionTime;
        }

        public int decodeDeletionTime(int deletionTime)
        {
            return baseDeletionTime + deletionTime;
        }

        public int encodeTTL(int ttl)
        {
            return ttl - baseTTL;
        }

        public int decodeTTL(int ttl)
        {
            return baseTTL + ttl;
        }
    }

    public static class FullHeader
    {
        public final Header header;
        public final boolean isEmpty;
        public final DeletionTime partitionDeletion;
        public final Row staticRow;
        public final int rowEstimate; // -1 if no estimate

        private FullHeader(Header header, boolean isEmpty, DeletionTime partitionDeletion, Row staticRow, int rowEstimate)
        {
            this.header = header;
            this.isEmpty = isEmpty;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.rowEstimate = rowEstimate;
        }
    }
}
