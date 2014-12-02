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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Serialize/deserialize a single Atom for the intra-node protocol.
 *
 * The encode format for an atom is <flags><clustering>(<row>|<marker>) where:
 *
 *   <flags> is a byte whose bits are flags. The rightmost 1st bit is only
 *       set to indicate the end of the partition. The 2nd bit indicates
 *       whether the reminder is a range tombstone marker (otherwise it's a row).
 *       If it's a row then the 3rd bit indicates if it's static, the 4th bit
 *       indicates the presence of a row timestamp and the 5th indicates the
 *       presence of complex deletion times.
 *       If it's a marker, then the 3rd bit indicates whether it's an open
 *       marker or not and the 4th bit indicates whether the EOC of the
 *       clustering is END (otherwise it's START).
 *   <clustering> is is the {@code clusteringSize} values of the clustering
 *       columns. Each value is an unsigned short for the length followed
 *       by length bytes. Note that static row are an exception and don't
 *       have this.
 *   <row> is [<timestamp>]<sc1>...<sci><cc1>...<ccj> where <timestamp> is
 *       the row timestamp whose presence is determined by the flags,
 *       <sci> is the simple columns of the row and <ccj> the complex ones.
 *       Each simple column <sci> is simply a <cell> (see below), while each
 *       <ccj> will be [<delTime>]<cell1>...<celln> where <delTime> is the
 *       deletion for this complex column (if flags indicates it present),
 *       <celln> are the <cell> for this complex column. The last "cell"
 *       will have it's presence flag unset to indicate the end of this column.
 *   <marker> is simply <delTime>, the deletion time of the marker.
 *
 *   <cell> A cell start with a 1 byte <flag>. Thre rightmost 1st bit indicates
 *       if there is actually a value for this cell. If this flag is unset,
 *       nothing more follows for the cell. 
 *       if it's a deleted cell, the 2nd one if it's an expiring one. The 3rd
 *       bit indicates if it's value is empty or not. Follows the <value> (unless
 *       it's marked empty in the flag) and a delta-encoded long <timestamp>.
 *       Then if it's a deleted or expiring cell a delta-encoded int <localDelTime>
 *       and if it's expiring a delta-encoded int <ttl>.
 */
public class AtomSerializer
{
    public static final AtomSerializer serializer = new AtomSerializer();

    // Atom flags
    private final static int END_OF_PARTITION     = 0x01;
    private final static int IS_MARKER            = 0x02;
    // For rows
    private final static int IS_STATIC            = 0x04;
    private final static int HAS_TIMESTAMP        = 0x08;
    private final static int HAS_COMPLEX_DELETION = 0x10;
    // For markers
    private final static int IS_OPEN              = 0x02;
    private final static int HAS_END_EOC          = 0x04;

    // Cell flags
    private final static int PRESENCE_MASK    = 0x01;
    private final static int DELETION_MASK    = 0x02;
    private final static int EXPIRATION_MASK  = 0x04;
    private final static int EMPTY_VALUE_MASK = 0x08;

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
        boolean isStatic = row.clustering() == EmptyClusteringPrefix.STATIC_PREFIX;
        long timestamp = row.timestamp();
        boolean hasComplexDeletion = row.hasComplexDeletion();
        if (isStatic)
            flags |= IS_STATIC;
        if (timestamp != Rows.NO_TIMESTAMP)
            flags |= HAS_TIMESTAMP;
        if (hasComplexDeletion)
            flags |= HAS_COMPLEX_DELETION;

        out.writeByte((byte)flags);
        if (!isStatic)
            ClusteringPrefix.serializer.serializeNoEOC(row.clustering(), out, version, header.clusteringTypes());

        if (timestamp != Rows.NO_TIMESTAMP)
            out.writeLong(header.encodeTimestamp(timestamp));

        Iterator<ColumnDefinition> simpleIter = header.simpleColumns(isStatic);
        while (simpleIter.hasNext())
            writeCell(row.getCell(simpleIter.next()), header, out);

        Iterator<ColumnDefinition> complexIter = header.complexColumns(isStatic);
        while (complexIter.hasNext())
        {
            ColumnDefinition column = complexIter.next();
            if (hasComplexDeletion)
                AtomIteratorSerializer.writeDelTime(row.getDeletion(column), header, out);

            Iterator<Cell> iter = row.getCells(column);
            if (iter != null)
            {
                while (iter.hasNext())
                    writeCell(iter.next(), header, out);
            }
            writeCell(null, header, out);
        }
    }

    public void serialize(RangeTombstoneMarker marker, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        int flags = IS_MARKER;
        if (marker.isOpenMarker())
            flags |= IS_OPEN;
        assert marker.clustering().eoc() != ClusteringPrefix.EOC.NONE;
        if (marker.clustering().eoc() == ClusteringPrefix.EOC.END)
            flags |= HAS_END_EOC;

        out.writeByte((byte)flags);
        ClusteringPrefix.serializer.serializeWithSizeNoEOC(marker.clustering(), out, version, header.clusteringTypes());
        AtomIteratorSerializer.writeDelTime(marker.delTime(), header, out);
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

        boolean isStatic = row.clustering() == EmptyClusteringPrefix.STATIC_PREFIX;
        long timestamp = row.timestamp();
        boolean hasComplexDeletion = row.hasComplexDeletion();

        if (!isStatic)
            size += ClusteringPrefix.serializer.serializedSizeNoEOC(row.clustering(), version, header.clusteringTypes(), sizes);

        if (timestamp != Rows.NO_TIMESTAMP)
            size += sizes.sizeof(header.encodeTimestamp(timestamp));

        Iterator<ColumnDefinition> simpleIter = header.simpleColumns(isStatic);
        while (simpleIter.hasNext())
            size += sizeOfCell(row.getCell(simpleIter.next()), header, sizes);

        Iterator<ColumnDefinition> complexIter = header.complexColumns(isStatic);
        while (complexIter.hasNext())
        {
            ColumnDefinition column = complexIter.next();
            if (hasComplexDeletion)
                size += AtomIteratorSerializer.delTimeSerializedSize(row.getDeletion(column), header, sizes);

            Iterator<Cell> iter = row.getCells(column);
            if (iter != null)
            {
                while (iter.hasNext())
                    size += sizeOfCell(iter.next(), header, sizes);
            }
            size += sizeOfCell(null, header, sizes);
        }

        return size;
    }

    public long serializedSize(RangeTombstoneMarker marker, SerializationHeader header, int version, TypeSizes sizes)
    {
        return 1 // flags
             + ClusteringPrefix.serializer.serializedSizeNoEOC(marker.clustering(), version, header.clusteringTypes(), sizes)
             + AtomIteratorSerializer.delTimeSerializedSize(marker.delTime(), header, sizes);
    }

    public void writeEndOfPartition(DataOutputPlus out) throws IOException
    {
        out.writeByte((byte)1);
    }

    public long serializedSizeEndOfPartition(TypeSizes sizes)
    {
        return 1;
    }

    private void writeCell(Cell cell, SerializationHeader header, DataOutputPlus out)
    throws IOException
    {
        if (cell == null)
        {
            out.writeByte((byte)0);
            return;
        }

        boolean hasValue = cell.value().hasRemaining();
        boolean isDeleted = isDeleted(cell);
        boolean isExpiring = isExpiring(cell);
        int flags = PRESENCE_MASK;
        if (!hasValue)
            flags |= EMPTY_VALUE_MASK;
        if (isDeleted)
            flags |= DELETION_MASK;
        else if (isExpiring)
            flags |= EXPIRATION_MASK;

        out.writeByte((byte)flags);

        if (hasValue)
            header.getType(cell.column()).writeValue(cell.value(), out);
        out.writeLong(header.encodeTimestamp(cell.timestamp()));
        if (isDeleted || isExpiring)
            out.writeInt(header.encodeDeletionTime(cell.localDeletionTime()));
        if (isExpiring)
            out.writeInt(header.encodeTTL(cell.ttl()));

        if (cell.column().isComplex())
            CellPath.serializer.serialize(cell.path(), out);
    }

    private long sizeOfCell(Cell cell, SerializationHeader header, TypeSizes sizes)
    {
        long size = 1; // flags

        if (cell == null)
            return size;

        boolean hasValue = cell.value().hasRemaining();
        boolean isDeleted = isDeleted(cell);
        boolean isExpiring = isExpiring(cell);

        if (hasValue)
            size += cell.column().type.writtenLength(cell.value(), sizes);

        size += sizes.sizeof(header.encodeTimestamp(cell.timestamp()));
        if (isDeleted || isExpiring)
            size += sizes.sizeof(header.encodeDeletionTime(cell.localDeletionTime()));
        if (isExpiring)
            size += sizes.sizeof(header.encodeTTL(cell.ttl()));

        if (cell.column().isComplex())
            size += CellPath.serializer.serializedSize(cell.path(), sizes);

        return size;
    }

    private boolean isDeleted(Cell cell)
    {
        return cell.localDeletionTime() < Cells.NO_DELETION_TIME && cell.ttl() == Cells.NO_TTL;
    }

    private boolean isExpiring(Cell cell)
    {
        return cell.ttl() != Cells.NO_TTL;
    }

    public Atom.Kind deserialize(DataInput in,
                                 SerializationHeader header,
                                 int version,
                                 ReusableClusteringPrefix clustering,
                                 Row.Writer rowWriter,
                                 RangeTombstoneMarker.Writer markerWriter)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if (isEndOfPartition(flags))
            return null;

        clustering.reset();
        if (kind(flags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
        {
            ClusteringPrefix.EOC eoc = eoc(flags);
            ClusteringPrefix.serializer.deserializeWithSizeNoEOC(in, eoc, version, header.clusteringTypes(), clustering.writer());
            deserializeMarker(in, header, version, clustering, flags, markerWriter);
            return Atom.Kind.RANGE_TOMBSTONE_MARKER;
        }
        else
        {
            if (!isStatic(flags))
                ClusteringPrefix.serializer.deserializeNoEOC(in, header.clusteringTypes().size(), ClusteringPrefix.EOC.NONE, version, header.clusteringTypes(), clustering.writer());

            deserializeRow(in, header, version, clustering, flags, rowWriter);
            return Atom.Kind.ROW;
        }
    }

    public void deserializeMarker(DataInput in,
                                  SerializationHeader header,
                                  int version,
                                  ClusteringPrefix clustering,
                                  int flags,
                                  RangeTombstoneMarker.Writer writer)
    throws IOException
    {
        boolean isOpen = isOpenMarker(flags);
        writer.writeMarker(clustering, isOpen, AtomIteratorSerializer.readDelTime(in, header));
    }

    public void skipMarker(DataInput in, SerializationHeader header, int version, int flags) throws IOException
    {
        AtomIteratorSerializer.skipDelTime(in, header);
    }

    public void deserializeRow(DataInput in,
                               SerializationHeader header,
                               int version,
                               ClusteringPrefix clustering,
                               int flags,
                               Row.Writer writer)
    throws IOException
    {
        boolean isStatic = isStatic(flags);
        boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
        boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

        if (!isStatic)
            writer.writeClustering(clustering);

        writer.writeTimestamp(hasTimestamp ? header.decodeTimestamp(in.readLong()) : Rows.NO_TIMESTAMP);

        Iterator<ColumnDefinition> simpleIter = header.simpleColumns(isStatic);
        while (simpleIter.hasNext())
            readCell(simpleIter.next(), in, header, writer);

        Iterator<ColumnDefinition> complexIter = header.complexColumns(isStatic);
        while (complexIter.hasNext())
        {
            ColumnDefinition column = complexIter.next();
            if (hasComplexDeletion)
                writer.writeComplexDeletion(column, AtomIteratorSerializer.readDelTime(in, header));

            while (readCell(column, in, header, writer));
        }
    }

    public void skipRow(DataInput in, SerializationHeader header, int version, int flags) throws IOException
    {
        boolean isStatic = isStatic(flags);
        boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
        boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

        // Note that we don't want want to use FileUtils.skipBytesFully for anything that may not have
        // the size we think due to VINT encoding
        if (hasTimestamp)
            in.readLong();

        Iterator<ColumnDefinition> simpleIter = header.simpleColumns(isStatic);
        while (simpleIter.hasNext())
            skipCell(simpleIter.next(), in, header);

        Iterator<ColumnDefinition> complexIter = header.complexColumns(isStatic);
        while (complexIter.hasNext())
        {
            ColumnDefinition column = complexIter.next();
            if (hasComplexDeletion)
                AtomIteratorSerializer.skipDelTime(in, header);

            while (skipCell(column, in, header));
        }
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
        return (flags & IS_STATIC) != 0;
    }

    public static boolean isOpenMarker(int flags)
    {
        return (flags & IS_OPEN) != 0;
    }

    public static ClusteringPrefix.EOC eoc(int flags)
    {
        return kind(flags) == Atom.Kind.ROW
             ? ClusteringPrefix.EOC.NONE
             : ((flags & HAS_END_EOC) != 0 ? ClusteringPrefix.EOC.END : ClusteringPrefix.EOC.START);
    }

    private boolean readCell(ColumnDefinition column, DataInput in, SerializationHeader header, Row.Writer writer)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & PRESENCE_MASK) == 0)
            return false;

        boolean hasValue = (flags & EMPTY_VALUE_MASK) == 0;
        boolean isDeleted = (flags & DELETION_MASK) != 0;
        boolean isExpiring = (flags & EXPIRATION_MASK) != 0;

        ByteBuffer value = hasValue ? header.getType(column).readValue(in) : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        long timestamp = header.decodeTimestamp(in.readLong());
        int localDelTime = isDeleted || isExpiring
                         ? header.decodeDeletionTime(in.readInt())
                         : Cells.NO_DELETION_TIME;
        int ttl = isExpiring
                ? header.decodeTTL(in.readInt())
                : Cells.NO_TTL;

        CellPath path = column.isComplex()
                      ? CellPath.serializer.deserialize(in)
                      : null;

        writer.writeCell(column, false, value, timestamp, localDelTime, ttl, path);
        return true;
    }

    private boolean skipCell(ColumnDefinition column, DataInput in, SerializationHeader header)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & PRESENCE_MASK) == 0)
            return false;

        boolean hasValue = (flags & EMPTY_VALUE_MASK) == 0;
        boolean isDeleted = (flags & DELETION_MASK) != 0;
        boolean isExpiring = (flags & EXPIRATION_MASK) != 0;

        if (hasValue)
            header.getType(column).skipValue(in);

        in.readLong(); // timestamp
        if (isDeleted || isExpiring)
            in.readInt(); // local deletion time
        if (isExpiring)
            in.readInt(); // ttl

        if (column.isComplex())
            CellPath.serializer.skip(in);

        return true;
    }
}
