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
public abstract class AtomDeserializer
{
    protected final CFMetaData metadata;
    protected final DataInput in;
    protected final LegacyLayout.Flag flag;
    protected final int expireBefore;
    protected final Descriptor.Version version;
    protected final Columns columns;

    protected AtomDeserializer(CFMetaData metadata,
                               DataInput in,
                               LegacyLayout.Flag flag,
                               int expireBefore,
                               Descriptor.Version version,
                               Columns columns)
    {
        this.metadata = metadata;
        this.in = in;
        this.flag = flag;
        this.expireBefore = expireBefore;
        this.version = version;
        this.columns = columns;
    }

    public static AtomDeserializer create(CFMetaData metadata,
                                          DataInput in,
                                          LegacyLayout.Flag flag,
                                          int expireBefore,
                                          Descriptor.Version version,
                                          Columns columns,
                                          SerializationHeader header)
    {
        if (version.storeRows)
            return new CurrentDeserializer(metadata, in, flag, expireBefore, version, columns, header);
        else
            return new LegacyLayout.LegacyAtomDeserializer(metadata, in, flag, expireBefore, version, columns);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Whether or not some atom has been read but not processed (neither readNext() nor
     * skipNext() has been called for that atom) yet.
     */
    public abstract boolean hasUnprocessed() throws IOException;

    /**
     * Compare the provided prefix to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public abstract int compareNextTo(Clusterable prefix) throws IOException;

    /**
     * Returns the next atom.
     */
    public abstract Atom readNext() throws IOException;

    /**
     * Skips the next atom.
     */
    public abstract void skipNext() throws IOException;

    private static class CurrentDeserializer extends AtomDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
        private boolean isReady;
        private boolean isDone;

        private final ReusableRow row;
        private final ReusableRangeTombstoneMarker marker;

        private CurrentDeserializer(CFMetaData metadata,
                                    DataInput in,
                                    LegacyLayout.Flag flag,
                                    int expireBefore,
                                    Descriptor.Version version,
                                    Columns columns,
                                    SerializationHeader header)
        {
            super(metadata, in, flag, expireBefore, version, columns);
            this.header = header;
            this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
            this.row = new ReusableRow(header.columns().regulars);
            this.marker = new ReusableRangeTombstoneMarker();
        }

        public boolean hasNext() throws IOException
        {
            if (hasUnprocessed())
                return true;

            prepareNext();
            return isDone;
        }

        private void prepareNext() throws IOException
        {
            if (isDone)
                return;

            nextFlags = in.readUnsignedByte();
            if (AtomSerializer.isEndOfPartition(nextFlags))
            {
                isDone = true;
                isReady = false;
                return;
            }

            clusteringDeserializer.prepare(nextFlags);
            isReady = true;
        }

        public boolean hasUnprocessed() throws IOException
        {
            return isReady;
        }

        public int compareNextTo(Clusterable prefix) throws IOException
        {
            return clusteringDeserializer.compareNextTo(prefix);
        }

        public Atom readNext() throws IOException
        {
            isReady = false;
            ClusteringPrefix nextClustering = clusteringDeserializer.readNext();
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                AtomSerializer.serializer.deserializeMarker(in, header, version.correspondingMessagingVersion, nextClustering, nextFlags, marker.writer());
                return marker;
            }
            else
            {
                AtomSerializer.serializer.deserializeRow(in, header, version.correspondingMessagingVersion, nextClustering, nextFlags, row.writer());
                return row;
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            clusteringDeserializer.skipNext();
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                AtomSerializer.serializer.skipMarker(in, header, version.correspondingMessagingVersion, nextFlags);
            }
            else
            {
                AtomSerializer.serializer.skipRow(in, header, version.correspondingMessagingVersion, nextFlags);
            }
        }
    }
}
