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

import java.nio.ByteBuffer;
import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.Descriptor;

/**
 * Helper class to deserialize OnDiskAtom efficiently.
 *
 * More precisely, this class is used by the low-level readers
 * (IndexedSliceReader and SSTableNamesIterator) to ensure we don't
 * do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public class AtomDeserializer
{
    private final CFMetaData metadata;
    private final LegacyLayout.Deserializer nameDeserializer;
    private final DataInput in;
    private final LegacyLayout.Flag flag;
    private final int expireBefore;
    private final Descriptor.Version version;
    private final Columns columns;

    private RangeTombstone openTombstone;

    private final ReusableRangeTombstoneMarker marker;
    private final ReusableRow row;

    private LegacyLayout.DeserializedCell cell;

    public AtomDeserializer(CFMetaData metadata,
                            DataInput in,
                            LegacyLayout.Flag flag,
                            int expireBefore,
                            Descriptor.Version version,
                            Columns columns)
    {
        this.metadata = metadata;
        this.nameDeserializer = metadata.layout().newDeserializer(in, version);
        this.in = in;
        this.flag = flag;
        this.expireBefore = expireBefore;
        this.version = version;
        this.columns = columns;
        this.marker = new ReusableRangeTombstoneMarker();
        this.row = new ReusableRow(columns);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public boolean hasNext() throws IOException
    {
        return hasUnprocessed() || nameDeserializer.hasNext();
    }

    /**
     * Whether or not some atom has been read but not processed (neither readNext() nor
     * skipNext() has been called for that atom) yet.
     */
    public boolean hasUnprocessed() throws IOException
    {
        return openTombstone != null || nameDeserializer.hasUnprocessed();
    }

    /**
     * Compare the provided prefix to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public int compareNextTo(Clusterable prefix) throws IOException
    {
        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
            return metadata.comparator.compare(openTombstone.max, prefix);

        return nameDeserializer.compareNextTo(prefix);
    }

    /**
     * Returns the next atom.
     */
    public Atom readNext() throws IOException
    {
        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
            return marker.setTo(openTombstone.max, false, openTombstone.data);

        ClusteringPrefix prefix = nameDeserializer.readNextClustering();
        int b = in.readUnsignedByte();
        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
        {
            // TODO: deal with new style RT
            openTombstone = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
            return marker.setTo(openTombstone.min, true, openTombstone.data);
        }

        Row.Writer writer = row.writer();
        writer.writeClustering(prefix);

        // If there is a row marker, it's the first cell
        ByteBuffer columnName = nameDeserializer.getNextColumnName();
        if (columnName != null && !columnName.hasRemaining())
        {
            metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
            writer.writeTimestamp(cell.timestamp());
        }
        else
        {
            writer.writeTimestamp(Long.MIN_VALUE);
            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
            if (columns.contains(column))
            {
                cell.column = column;
                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
                Cells.write(cell, writer);
            }
            else
            {
                metadata.layout().skipCellBody(in, b);
            }
        }

        // Read the rest of the cells belonging to this CQL row
        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
        {
            nameDeserializer.readNextClustering();
            b = in.readUnsignedByte();
            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
            {
                if (!columns.contains(column))
                {
                    metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
                    continue;
                }

                // This is a collection tombstone
                RangeTombstone rt = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
                // TODO: we could assert that the min and max are what we think they are. Just in case
                // someone thrift side has done something *really* nasty.
                writer.writeComplexDeletion(column, rt.data);
            }
            else
            {
                if (!columns.contains(column))
                {
                    metadata.layout().skipCellBody(in, b);
                    continue;
                }

                cell.column = column;
                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
                Cells.write(cell, writer);
            }
        }
        return row;
    }

    private ColumnDefinition getDefinition(ByteBuffer columnName)
    {
        // For non-CQL3 layouts, every defined column metadata is handled by the static row
        if (!metadata.layout().isCQL3Layout())
            return metadata.compactValueColumn();

        return metadata.getColumnDefinition(columnName);
    }

    /**
     * Skips the next atom.
     */
    public void skipNext() throws IOException
    {
        ClusteringPrefix prefix = nameDeserializer.readNextClustering();
        int b = in.readUnsignedByte();
        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
        {
            metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
            return;
        }

        metadata.layout().skipCellBody(in, b);

        // Skip the rest of the cells belonging to this CQL row
        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
        {
            nameDeserializer.skipNext();
            b = in.readUnsignedByte();
            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
                metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
            else
                metadata.layout().skipCellBody(in, b);
        }
    }
}
