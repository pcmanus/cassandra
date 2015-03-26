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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Serialize/deserialize a single Atom for the intra-node protocol.
 *
 * The encode format for an atom is <flags>(<row>|<marker>) where:
 *
 *   <flags> is a byte whose bits are flags. The rightmost 1st bit is only
 *       set to indicate the end of the partition. The 2nd bit indicates
 *       whether the reminder is a range tombstone marker (otherwise it's a row).
 *       If it's a row then the 3rd bit indicates if it's static, the 4th bit
 *       indicates the presence of a row timestamp, the 5th the presence of a row
 *       ttl, the 6th the presence of row deletion and the 7th indicates the
 *       presence of complex deletion times.
 *       If it's a marker, then the 3rd bit indicates whether it's a start
 *       marker or not and the 4th bit indicates whether it's an inclusive
 *       one or not.
 *   <row> is <clustering>[<timestamp>][<ttl>][<deletion>][<maxLiveTimestamp>]<sc1>...<sci><cc1>...<ccj> where
 *       <clustering> is the row clustering as serialized by
 *       {@code Clustering.serializer}. Note that static row are an exception and
 *       don't have this. <timestamp>, <ttl> and <deletion> are the row timestamp, ttl and deletion
 *       whose presence is determined by the flags, <maxLiveTimestamp> is the max live timestamp
 *       for the row (it's always present when serializing on-wire, but it's never present
 *       for sstables as we compute it on reads). <sci> is the simple columns of the row and <ccj> the
 *       complex ones.  There is actually 2 slightly different possible layout for those
 *       cell: a dense one and a sparse one. Which one is used depends on the serialization
 *       header and more precisely of {@link SerializationHeader.useSparseColumnLayout()}:
 *         1) in the dense layout, there will be as many <sci> and <ccj> as there is columns
 *            in the serialization header. *Each simple column <sci> will simply be a <cell>
 *            (which might have no value, see below), while each <ccj> will be
 *             [<delTime>]<cell1>...<celln><emptyCell> where <delTime> is the deletion for
 *             this complex column (if flags indicates it present), <celln> are the <cell>
 *             for this complex column and <emptyCell> is a last cell that will have no value
 *             to indicate the end of this column.
 *         2) in the sparse layout, there won't be "empty" cells, i.e. only the column that
 *            actually have a cell are represented. For that, each <sci> and <ccj> start
 *            by a 2 byte index that points to the column in the header it belongs to. After
 *            that, each <sci> and <ccj> is the same than for the dense layout. But contrarily
 *            to the dense layout we won't know how many elements are serialized so a 2 byte
 *            marker with a value of -1 will indicates the end of the row.
 *   <marker> is <bound><deletion> where <bound> is the marker bound as serialized
 *       by {@code Slice.Bound.serializer} and <deletion> is the marker deletion
 *       time.
 *
 *   <cell> A cell start with a 1 byte <flag>. Thre rightmost 1st bit indicates
 *       if there is actually a value for this cell. If this flag is unset,
 *       nothing more follows for the cell. The 2nd and third flag indicates if
 *       it's a deleted or expiring cell. The 4th flag indicates if the value
 *       is empty or not. The 5th and 6th indicates if the timestamp and ttl/
 *       localDeletionTime for the cell are the same than the row one (if that
 *       is the case, those are not repeated for the cell).Follows the <value>
 *       (unless it's marked empty in the flag) and a delta-encoded long <timestamp>
 *       (unless the flag tells to use the row level one).
 *       Then if it's a deleted or expiring cell a delta-encoded int <localDelTime>
 *       and if it's expiring a delta-encoded int <ttl> (unless it's an expiring cell
 *       and the ttl and localDeletionTime are indicated by the flags to be the same
 *       than the row ones, in which case none of those appears).
 */
public class AtomSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(AtomSerializer.class);

    public static final AtomSerializer serializer = new AtomSerializer();

    // Atom flags
    private final static int END_OF_PARTITION     = 0x01;
    private final static int IS_MARKER            = 0x02;
    // For rows
    private final static int IS_STATIC            = 0x04;
    private final static int HAS_TIMESTAMP        = 0x08;
    private final static int HAS_TTL              = 0x10;
    private final static int HAS_DELETION         = 0x20;
    private final static int HAS_COMPLEX_DELETION = 0x40;
    // For markers
    private final static int IS_START             = 0x04;
    private final static int IS_INCLUSIVE         = 0x08;

    // Cell flags
    private final static int PRESENCE_MASK     = 0x01;
    private final static int DELETION_MASK     = 0x02;
    private final static int EXPIRATION_MASK   = 0x04;
    private final static int EMPTY_VALUE_MASK  = 0x08;
    private final static int USE_ROW_TIMESTAMP = 0x10;
    private final static int USE_ROW_TTL       = 0x20;

    public void serialize(Atom atom, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        if (atom.kind() == Atom.Kind.RANGE_TOMBSTONE_MARKER)
        {
            serialize((RangeTombstoneMarker)atom, header, out, version);
        }
        else
        {
            serialize((Row)atom, header, out, version);
        }
    }

    public void serialize(Row row, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        int flags = 0;
        boolean isStatic = row.isStatic();

        LivenessInfo pkLiveness = row.partitionKeyLivenessInfo();
        DeletionTime deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();

        if (isStatic)
            flags |= IS_STATIC;
        if (pkLiveness.hasTimestamp())
            flags |= HAS_TIMESTAMP;
        if (pkLiveness.hasTTL())
            flags |= HAS_TTL;
        if (!deletion.isLive())
            flags |= HAS_DELETION;
        if (hasComplexDeletion)
            flags |= HAS_COMPLEX_DELETION;

        out.writeByte((byte)flags);
        if (!isStatic)
            Clustering.serializer.serialize(row.clustering(), out, version, header.clusteringTypes());

        if (pkLiveness.hasTimestamp())
            out.writeLong(header.encodeTimestamp(pkLiveness.timestamp()));
        if (pkLiveness.hasTTL())
        {
            out.writeInt(header.encodeTTL(pkLiveness.ttl()));
            out.writeInt(header.encodeDeletionTime(pkLiveness.localDeletionTime()));
        }
        if (!deletion.isLive())
            AtomIteratorSerializer.writeDelTime(deletion, header, out);

        if (!header.isForSSTable())
            out.writeLong(header.encodeTimestamp(row.maxLiveTimestamp()));

        Columns columns = header.columns(isStatic);
        int simpleCount = columns.simpleColumnCount();
        boolean useSparse = header.useSparseColumnLayout(isStatic);

        for (int i = 0; i < simpleCount; i++)
            writeSimpleColumn(i, columns.getSimple(i), row, header, out, pkLiveness, useSparse);

        for (int i = simpleCount; i < columns.columnCount(); i++)
            writeComplexColumn(i, columns.getComplex(i - simpleCount), row, hasComplexDeletion, header, out, pkLiveness, useSparse);

        if (useSparse)
            out.writeShort(-1);
    }

    private void writeSimpleColumn(int idx, ColumnDefinition column, Row row, SerializationHeader header, DataOutputPlus out, LivenessInfo rowLiveness, boolean useSparse)
    throws IOException
    {
        Cell cell = row.getCell(column);
        if (useSparse)
        {
            if (cell == null)
                return;

            out.writeShort(idx);
        }

        writeCell(cell, header, out, rowLiveness);
    }

    private void writeComplexColumn(int idx, ColumnDefinition column, Row row, boolean hasComplexDeletion, SerializationHeader header, DataOutputPlus out, LivenessInfo rowLiveness, boolean useSparse)
    throws IOException
    {
        Iterator<Cell> cells = row.getCells(column);
        DeletionTime deletion = row.getDeletion(column);

        if (useSparse)
        {
            assert hasComplexDeletion || deletion.isLive();
            if (cells == null && deletion.isLive())
                return;

            out.writeShort(idx);
        }

        if (hasComplexDeletion)
            AtomIteratorSerializer.writeDelTime(deletion, header, out);

        if (cells != null)
            while (cells.hasNext())
                writeCell(cells.next(), header, out, rowLiveness);

        writeCell(null, header, out, rowLiveness);
    }

    public void serialize(RangeTombstoneMarker marker, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        int flags = IS_MARKER;
        if (marker.clustering().isStart())
            flags |= IS_START;
        if (marker.clustering().isInclusive())
            flags |= IS_INCLUSIVE;

        out.writeByte((byte)flags);
        Slice.Bound.serializer.serializeValues(marker.clustering(), out, version, header.clusteringTypes());
        AtomIteratorSerializer.writeDelTime(marker.deletionTime(), header, out);
    }

    public long serializedSize(Atom atom, SerializationHeader header, int version, TypeSizes sizes)
    {
        return atom.kind() == Atom.Kind.RANGE_TOMBSTONE_MARKER
             ? serializedSize((RangeTombstoneMarker)atom, header, version, sizes)
             : serializedSize((Row)atom, header, version, sizes);
    }

    public long serializedSize(Row row, SerializationHeader header, int version, TypeSizes sizes)
    {
        long size = 1; // flags

        boolean isStatic = row.isStatic();
        LivenessInfo pkLiveness = row.partitionKeyLivenessInfo();
        DeletionTime deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();

        if (!isStatic)
            size += Clustering.serializer.serializedSize(row.clustering(), version, header.clusteringTypes(), sizes);

        if (pkLiveness.hasTimestamp())
            size += sizes.sizeof(header.encodeTimestamp(pkLiveness.timestamp()));
        if (pkLiveness.hasTTL())
        {
            size += sizes.sizeof(header.encodeTTL(pkLiveness.ttl()));
            size += sizes.sizeof(header.encodeDeletionTime(pkLiveness.localDeletionTime()));
        }
        if (!deletion.isLive())
            size += AtomIteratorSerializer.delTimeSerializedSize(deletion, header, sizes);

        if (!header.isForSSTable())
            size += sizes.sizeof(header.encodeTimestamp(row.maxLiveTimestamp()));

        Columns columns = header.columns(isStatic);
        int simpleCount = columns.simpleColumnCount();
        boolean useSparse = header.useSparseColumnLayout(isStatic);

        for (int i = 0; i < simpleCount; i++)
            size += sizeOfSimpleColumn(i, columns.getSimple(i), row, header, sizes, pkLiveness, useSparse);

        for (int i = simpleCount; i < columns.columnCount(); i++)
            size += sizeOfComplexColumn(i, columns.getComplex(i - simpleCount), row, hasComplexDeletion, header, sizes, pkLiveness, useSparse);

        if (useSparse)
            size += sizes.sizeof((short)-1);

        return size;
    }

    private long sizeOfSimpleColumn(int idx, ColumnDefinition column, Row row, SerializationHeader header, TypeSizes sizes, LivenessInfo rowLiveness, boolean useSparse)
    {
        long size = 0;
        Cell cell = row.getCell(column);
        if (useSparse)
        {
            if (cell == null)
                return size;

            size += sizes.sizeof((short)idx);
        }
        return size + sizeOfCell(cell, header, sizes, rowLiveness);
    }

    private long sizeOfComplexColumn(int idx, ColumnDefinition column, Row row, boolean hasComplexDeletion, SerializationHeader header, TypeSizes sizes, LivenessInfo rowLiveness, boolean useSparse)
    {
        long size = 0;
        Iterator<Cell> cells = row.getCells(column);
        DeletionTime deletion = row.getDeletion(column);
        if (useSparse)
        {
            assert hasComplexDeletion || deletion.isLive();
            if (cells == null && deletion.isLive())
                return size;

            size += sizes.sizeof((short)idx);
        }

        if (hasComplexDeletion)
            size += AtomIteratorSerializer.delTimeSerializedSize(deletion, header, sizes);

        if (cells != null)
            while (cells.hasNext())
                size += sizeOfCell(cells.next(), header, sizes, rowLiveness);

        return size + sizeOfCell(null, header, sizes, rowLiveness);
    }

    public long serializedSize(RangeTombstoneMarker marker, SerializationHeader header, int version, TypeSizes sizes)
    {
        return 1 // flags
             + Slice.Bound.serializer.serializedSizeValues(marker.clustering(), version, header.clusteringTypes(), sizes)
             + AtomIteratorSerializer.delTimeSerializedSize(marker.deletionTime(), header, sizes);
    }

    public void writeEndOfPartition(DataOutputPlus out) throws IOException
    {
        out.writeByte((byte)1);
    }

    public long serializedSizeEndOfPartition(TypeSizes sizes)
    {
        return 1;
    }

    public Atom.Kind deserialize(DataInput in,
                                 SerializationHeader header,
                                 SerializationHelper helper,
                                 Row.Writer rowWriter,
                                 RangeTombstoneMarker.Writer markerWriter)
    throws IOException
    {
        return deserialize(in, header, helper, null, rowWriter, markerWriter);
    }

    public Atom.Kind deserialize(DataInput in,
                                 SerializationHeader header,
                                 SerializationHelper helper,
                                 Columns columnsToFetch,
                                 Row.Writer rowWriter,
                                 RangeTombstoneMarker.Writer markerWriter)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if (isEndOfPartition(flags))
            return null;

        if (kind(flags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
        {
            Slice.Bound.Kind kind = boundKind(flags);
            Slice.Bound.serializer.deserializeValues(in, kind, helper.version, header.clusteringTypes(), markerWriter);
            deserializeMarkerBody(in, header, helper, flags, markerWriter);
            return Atom.Kind.RANGE_TOMBSTONE_MARKER;
        }
        else
        {
            assert !isStatic(flags); // deserializeStaticRow should be used for that.
            Clustering.serializer.deserialize(in, helper.version, header.clusteringTypes(), rowWriter);
            deserializeRowBody(in, header, helper, columnsToFetch, flags, rowWriter);
            return Atom.Kind.ROW;
        }
    }

    public Row deserializeStaticRow(DataInput in, SerializationHeader header, SerializationHelper helper)
    throws IOException
    {
        return deserializeStaticRow(in, header, helper, null);
    }

    public Row deserializeStaticRow(DataInput in, SerializationHeader header, SerializationHelper helper, Columns columnsToFetch)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        assert !isEndOfPartition(flags) && kind(flags) == Atom.Kind.ROW && isStatic(flags);
        StaticRow.Builder builder = StaticRow.builder(header.columns().statics, helper.nowInSec);
        deserializeRowBody(in, header, helper, columnsToFetch, flags, builder);
        return builder.build();
    }

    public void skipStaticRow(DataInput in, SerializationHeader header, SerializationHelper helper) throws IOException
    {
        int flags = in.readUnsignedByte();
        assert !isEndOfPartition(flags) && kind(flags) == Atom.Kind.ROW && isStatic(flags);
        skipRowBody(in, header, helper, flags);
    }

    public void deserializeMarkerBody(DataInput in,
                                      SerializationHeader header,
                                      SerializationHelper helper,
                                      int flags,
                                      RangeTombstoneMarker.Writer writer)
    throws IOException
    {
        writer.writeMarkerDeletion(AtomIteratorSerializer.readDelTime(in, header));
        writer.endOfMarker();
    }

    public void skipMarkerBody(DataInput in, SerializationHeader header, int flags) throws IOException
    {
        AtomIteratorSerializer.skipDelTime(in, header);
    }

    public void deserializeRowBody(DataInput in,
                                   SerializationHeader header,
                                   SerializationHelper helper,
                                   int flags,
                                   Row.Writer writer)
    throws IOException
    {
        deserializeRowBody(in, header, helper, null, flags, writer);
    }

    public void deserializeRowBody(DataInput in,
                                   SerializationHeader header,
                                   SerializationHelper helper,
                                   Columns columnsToFetch,
                                   int flags,
                                   Row.Writer writer)
    throws IOException
    {
        boolean isStatic = isStatic(flags);
        boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
        boolean hasTTL = (flags & HAS_TTL) != 0;
        boolean hasDeletion = (flags & HAS_DELETION) != 0;
        boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

        long timestamp = hasTimestamp ? header.decodeTimestamp(in.readLong()) : LivenessInfo.NO_TIMESTAMP;
        int ttl = hasTTL ? header.decodeTTL(in.readInt()) : LivenessInfo.NO_TTL;
        int localDeletionTime = hasTTL ? header.decodeDeletionTime(in.readInt()) : LivenessInfo.NO_DELETION_TIME;
        DeletionTime deletion = hasDeletion ? AtomIteratorSerializer.readDelTime(in, header) : DeletionTime.LIVE;

        helper.writePartitionKeyLivenessInfo(writer, timestamp, ttl, localDeletionTime);
        writer.writeRowDeletion(deletion);

        long maxLiveTimestamp = header.isForSSTable() ? LivenessInfo.NO_TIMESTAMP : header.decodeTimestamp(in.readLong());

        Columns columns = header.columns(isStatic);
        if (header.useSparseColumnLayout(isStatic))
        {
            int simpleCount = columns.simpleColumnCount();
            int i;
            while ((i = in.readShort()) >= 0)
            {
                if (i < simpleCount)
                    readSimpleColumn(columns.getSimple(i), in, header, helper, columnsToFetch, writer);
                else
                    readComplexColumn(columns.getComplex(i - simpleCount), in, header, helper, hasComplexDeletion, columnsToFetch, writer);
            }
        }
        else
        {
            for (int i = 0; i < columns.simpleColumnCount(); i++)
                readSimpleColumn(columns.getSimple(i), in, header, helper, columnsToFetch, writer);

            for (int i = 0; i < columns.complexColumnCount(); i++)
                readComplexColumn(columns.getComplex(i), in, header, helper, hasComplexDeletion, columnsToFetch, writer);
        }

        writer.writeMaxLiveTimestamp(Math.max(maxLiveTimestamp, helper.getMaxLiveTimestamp()));
        writer.endOfRow();
    }

    private void readSimpleColumn(ColumnDefinition column, DataInput in, SerializationHeader header, SerializationHelper helper, Columns columnsToFetch, Row.Writer writer)
    throws IOException
    {
        if (columnsToFetch == null || columnsToFetch.contains(column))
            readCell(column, in, header, helper, writer);
        else
            skipCell(column, in, header, helper);
    }

    private void readComplexColumn(ColumnDefinition column, DataInput in, SerializationHeader header, SerializationHelper helper, boolean hasComplexDeletion, Columns columnsToFetch, Row.Writer writer)
    throws IOException
    {
        if (columnsToFetch == null || columnsToFetch.contains(column))
        {
            if (hasComplexDeletion)
                writer.writeComplexDeletion(column, AtomIteratorSerializer.readDelTime(in, header));

            while (readCell(column, in, header, helper, writer));
        }
        else
        {
            skipComplexColumn(column, in, header, helper, hasComplexDeletion);
        }
    }

    public void skipRowBody(DataInput in, SerializationHeader header, SerializationHelper helper, int flags) throws IOException
    {
        boolean isStatic = isStatic(flags);
        boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
        boolean hasTTL = (flags & HAS_TTL) != 0;
        boolean hasDeletion = (flags & HAS_DELETION) != 0;
        boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

        // Note that we don't want want to use FileUtils.skipBytesFully for anything that may not have
        // the size we think due to VINT encoding
        if (hasTimestamp)
            in.readLong();
        if (hasTTL)
        {
            // ttl and localDeletionTime
            in.readInt();
            in.readInt();
        }
        if (hasDeletion)
            AtomIteratorSerializer.skipDelTime(in, header);

        // maxLiveTimestamp
        if (!header.isForSSTable())
            in.readLong();

        Columns columns = header.columns(isStatic);
        int simpleCount = columns.simpleColumnCount();
        if (header.useSparseColumnLayout(isStatic))
        {
            int i;
            while ((i = in.readShort()) >= 0)
            {
                if (i < simpleCount)
                    skipCell(columns.getSimple(i), in, header, helper);
                else
                    skipComplexColumn(columns.getComplex(i - simpleCount), in, header, helper, hasComplexDeletion);
            }
        }
        else
        {
            for (int i = 0; i < columns.simpleColumnCount(); i++)
                skipCell(columns.getSimple(i), in, header, helper);

            for (int i = 0; i < columns.complexColumnCount(); i++)
                skipComplexColumn(columns.getComplex(i - simpleCount), in, header, helper, hasComplexDeletion);
        }
    }

    private void skipComplexColumn(ColumnDefinition column, DataInput in, SerializationHeader header, SerializationHelper helper, boolean hasComplexDeletion)
    throws IOException
    {
        if (hasComplexDeletion)
            AtomIteratorSerializer.skipDelTime(in, header);

        while (skipCell(column, in, header, helper));
    }

    public static boolean isEndOfPartition(int flags)
    {
        return (flags & END_OF_PARTITION) != 0;
    }

    public static Atom.Kind kind(int flags)
    {
        return (flags & IS_MARKER) != 0 ? Atom.Kind.RANGE_TOMBSTONE_MARKER : Atom.Kind.ROW;
    }

    public static boolean isStatic(int flags)
    {
        return (flags & IS_MARKER) == 0 && (flags & IS_STATIC) != 0;
    }

    public static ClusteringPrefix.Kind boundKind(int flags)
    {
        return Slice.Bound.boundKind((flags & IS_START) != 0, (flags & IS_INCLUSIVE) != 0);
    }

    private void writeCell(Cell cell, SerializationHeader header, DataOutputPlus out, LivenessInfo rowLiveness)
    throws IOException
    {
        if (cell == null)
        {
            out.writeByte((byte)0);
            return;
        }

        boolean hasValue = cell.value().hasRemaining();
        boolean isDeleted = cell.isTombstone();
        boolean isExpiring = cell.isExpiring();
        boolean useRowTimestamp = rowLiveness.hasTimestamp() && cell.livenessInfo().timestamp() == rowLiveness.timestamp();
        boolean useRowTTL = isExpiring && rowLiveness.hasTTL() && cell.livenessInfo().ttl() == rowLiveness.ttl() && cell.livenessInfo().localDeletionTime() == rowLiveness.localDeletionTime();
        int flags = PRESENCE_MASK;
        if (!hasValue)
            flags |= EMPTY_VALUE_MASK;

        if (isDeleted)
            flags |= DELETION_MASK;
        else if (isExpiring)
            flags |= EXPIRATION_MASK;

        if (useRowTimestamp)
            flags |= USE_ROW_TIMESTAMP;
        if (useRowTTL)
            flags |= USE_ROW_TTL;

        out.writeByte((byte)flags);

        if (hasValue)
            header.getType(cell.column()).writeValue(cell.value(), out);

        if (!useRowTimestamp)
            out.writeLong(header.encodeTimestamp(cell.livenessInfo().timestamp()));

        if ((isDeleted || isExpiring) && !useRowTTL)
            out.writeInt(header.encodeDeletionTime(cell.livenessInfo().localDeletionTime()));
        if (isExpiring && !useRowTTL)
            out.writeInt(header.encodeTTL(cell.livenessInfo().ttl()));

        if (cell.column().isComplex())
            cell.column().cellPathSerializer().serialize(cell.path(), out);
    }

    private long sizeOfCell(Cell cell, SerializationHeader header, TypeSizes sizes, LivenessInfo rowLiveness)
    {
        long size = 1; // flags

        if (cell == null)
            return size;

        boolean hasValue = cell.value().hasRemaining();
        boolean isDeleted = cell.isTombstone();
        boolean isExpiring = cell.isExpiring();
        boolean useRowTimestamp = rowLiveness.hasTimestamp() && cell.livenessInfo().timestamp() == rowLiveness.timestamp();
        boolean useRowTTL = isExpiring && rowLiveness.hasTTL() && cell.livenessInfo().ttl() == rowLiveness.ttl() && cell.livenessInfo().localDeletionTime() == rowLiveness.localDeletionTime();

        if (hasValue)
            size += header.getType(cell.column()).writtenLength(cell.value(), sizes);

        if (!useRowTimestamp)
            size += sizes.sizeof(header.encodeTimestamp(cell.livenessInfo().timestamp()));

        if ((isDeleted || isExpiring) && !useRowTTL)
            size += sizes.sizeof(header.encodeDeletionTime(cell.livenessInfo().localDeletionTime()));
        if (isExpiring && !useRowTTL)
            size += sizes.sizeof(header.encodeTTL(cell.livenessInfo().ttl()));

        if (cell.column().isComplex())
            size += cell.column().cellPathSerializer().serializedSize(cell.path(), sizes);

        return size;
    }

    private boolean readCell(ColumnDefinition column, DataInput in, SerializationHeader header, SerializationHelper helper, Row.Writer writer)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & PRESENCE_MASK) == 0)
            return false;

        boolean hasValue = (flags & EMPTY_VALUE_MASK) == 0;
        boolean isDeleted = (flags & DELETION_MASK) != 0;
        boolean isExpiring = (flags & EXPIRATION_MASK) != 0;
        boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP) != 0;
        boolean useRowTTL = (flags & USE_ROW_TTL) != 0;

        ByteBuffer value = hasValue ? header.getType(column).readValue(in) : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long timestamp = useRowTimestamp ? helper.getRowTimestamp() : header.decodeTimestamp(in.readLong());

        int localDelTime = useRowTTL
                         ? helper.getRowLocalDeletionTime()
                         : (isDeleted || isExpiring ? header.decodeDeletionTime(in.readInt()) : LivenessInfo.NO_DELETION_TIME);

        int ttl = useRowTTL
                ? helper.getRowTTL()
                : (isExpiring ? header.decodeTTL(in.readInt()) : LivenessInfo.NO_TTL);

        CellPath path = column.isComplex()
                      ? column.cellPathSerializer().deserialize(in)
                      : null;

        helper.writeCell(writer, column, false, value, timestamp, localDelTime, ttl, path);

        return true;
    }

    private boolean skipCell(ColumnDefinition column, DataInput in, SerializationHeader header, SerializationHelper helper)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & PRESENCE_MASK) == 0)
            return false;

        boolean hasValue = (flags & EMPTY_VALUE_MASK) == 0;
        boolean isDeleted = (flags & DELETION_MASK) != 0;
        boolean isExpiring = (flags & EXPIRATION_MASK) != 0;
        boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP) != 0;
        boolean useRowTTL = (flags & USE_ROW_TTL) != 0;

        if (hasValue)
            header.getType(column).skipValue(in);

        long timestamp = useRowTimestamp ? helper.getRowTimestamp() : header.decodeTimestamp(in.readLong());
        int localDelTime = useRowTTL
                         ? helper.getRowLocalDeletionTime()
                         : (isDeleted || isExpiring ? header.decodeDeletionTime(in.readInt()) : LivenessInfo.NO_DELETION_TIME);
        helper.updateForSkippedCell(timestamp, localDelTime);

        if (isExpiring && !useRowTTL)
            in.readInt(); // ttl

        if (column.isComplex())
            column.cellPathSerializer().skip(in);

        return true;
    }
}
