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

    public void serialize(Atom atom, AtomIteratorSerializer.Header header, DataOutputPlus out, int version)
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

    public void serialize(Row row, AtomIteratorSerializer.Header header, DataOutputPlus out, int version)
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
            if (isStatic)
                ClusteringPrefix.serializer.serializeNoEOC(row.clustering(), out, version);

            if (timestamp != Rows.NO_TIMESTAMP)
                out.writeLong(timestamp - header.stats.minTimestamp);

            Columns columns = isStatic ? header.columns.statics : header.columns.regulars;
            for (int i = 0; i < columns.simpleColumnCount(); i++)
                writeCell(row.getCell(columns.getSimple(i)), header, out);

            for (int i = 0; i < columns.complexColumnCount(); i++)
            {
                ColumnDefinition column = columns.getComplex(i);
                if (hasComplexDeletion)
                    AtomIteratorSerializer.writeDelTime(row.getDeletion(column), header.stats, out);

                Iterator<Cell> iter = row.getCells(column);
                while (iter.hasNext())
                    writeCell(iter.next(), header, out);
                writeCell(null, header, out);
            }
    }

    public void serialize(RangeTombstoneMarker marker, AtomIteratorSerializer.Header header, DataOutputPlus out, int version)
    {
            int flags = IS_MARKER;
            if (marker.isOpenMarker())
                flags |= IS_OPEN;
            assert marker.clustering().eoc() != ClusteringPrefix.EOC.NONE;
            if (marker.clustering().eoc() == ClusteringPrefix.EOC.END)
                flags |= HAS_END_EOC;

            out.writeByte((byte)flags);
            ClusteringPrefix.serializer.serializeNoEOC(marker.clustering(), out, version);
            AtomIteratorSerializer.writeDelTime(marker.delTime(), header.stats, out);
    }

    public static void writeEndOfPartition(DataOutputPlus out)
    {
        out.writeByte((byte)1);
    }

    private void writeCell(Cell cell, AtomIteratorSerializer.Header header, DataOutputPlus out)
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

        if (hasValue)
            cell.column().type.writeValue(cell.value(), out);
        out.writeLong(cell.timestamp() - header.stats.minTimestamp);
        if (isDeleted || isExpiring)
            out.writeInt(cell.localDeletionTime() - header.stats.minLocalDeletionTime);
        if (isExpiring)
            out.writeInt(cell.ttl() - header.stats.minTTL);

        if (cell.column().isComplex())
            CellPath.serializer.serialize(cell.path(), out);
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
                                 AtomIteratorSerializer.Header header,
                                 int version,
                                 Row.Writer rowWriter,
                                 RangeTombstoneMarker.Writer markerWriter)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & END_OF_PARTITION) != 0)
            return null;

        ReusableClusteringPrefix clustering = header.reusableClustering();
        if ((flags & IS_MARKER) != 0)
        {
            boolean isOpen = (flags & IS_OPEN) != 0;
            ClusteringPrefix.EOC eoc = (flags & HAS_END_EOC) != 0 ? ClusteringPrefix.EOC.END : ClusteringPrefix.EOC.START;

            ClusteringPrefix.serializer.deserializeNoEOC(in, header.metadata.clusteringColumns().size(), eoc, version, clustering.writer());
            markerWriter.writeMarker(clustering, isOpen, AtomIteratorSerializer.readDelTime(in, header.stats));
            return Atom.Kind.RANGE_TOMBSTONE_MARKER;
        }
        else
        {
            boolean isStatic = (flags & IS_STATIC) != 0;
            boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
            boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

            if (!isStatic)
            {
                ClusteringPrefix.serializer.deserializeNoEOC(in, header.metadata.clusteringColumns().size(), ClusteringPrefix.EOC.NONE, version, clustering.writer());
                rowWriter.writeClustering(clustering);
            }
            rowWriter.writeTimestamp(hasTimestamp ? header.stas.minTimestamp + in.readLong() : Rows.NO_TIMESTAMP);

            Columns columns = isStatic ? header.columns.statics : header.columns.regulars;
            for (int i = 0; i < columns.simpleColumnCount(); i++)
                readCell(columns.getSimple(i), in, header, rowWriter);

            for (int i = 0; i < columns.complexColumnCount(); i++)
            {
                ColumnDefinition column = columns.getComplex(i);
                if (hasComplexDeletion)
                    rowWriter.writeComplexDeletion(column, AtomIteratorSerializer.readDelTime(in, header.stats));

                while (readCell(column, in, header, rowWriter));
            }

            return Atom.Kind.ROW;
        }
    }

    private boolean readCell(ColumnDefinition column, DataInput in, AtomIteratorSerializer.Header header, Row.Writer writer)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        if ((flags & PRESENCE_MASK) == 0)
            return false;

        boolean hasValue = (flags & EMPTY_VALUE_MASK) == 0;
        boolean isDeleted = (flags & DELETION_MASK) != 0;
        boolean isExpiring = (flags & EXPIRATION_MASK) != 0;

        ByteBuffer value = hasValue ? column.type.readValue(in) : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        long timestamp = header.stats.minTimestamp + in.readLong();
        int localDelTime = isDeleted || isExpiring
                         ? header.stas.minLocalDeletionTime + in.readInt()
                         : Cells.NO_DELETION_TIME;
        int ttl = isExpiring
                ? header.stats.minTTL + in.readInt()
                : Cells.NO_TTL;

        CellPath path = column.isComplex()
                      ? CellPath.serializer.deserialize(in)
                      : null;

        writer.writeCell(column, false, value, timestamp, localDelTime, ttl, path);
        return true;
    }
}
