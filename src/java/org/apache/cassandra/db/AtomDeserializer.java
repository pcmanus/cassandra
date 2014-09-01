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

import java.io.DataInput;
import java.io.IOException;

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
    private final LegacyLayout layout;
    private final LegacyLayout.Deserializer nameDeserializer;
    private final DataInput in;
    private final LegacyLayout.Flag flag;
    private final int expireBefore;
    private final Descriptor.Version version;

    public AtomDeserializer(LegacyLayout layout,
                            DataInput in,
                            LegacyLayout.Flag flag,
                            int expireBefore,
                            Descriptor.Version version)
    {
        this.layout = layout;
        this.nameDeserializer = layout.newDeserializer(in, version);
        this.in = in;
        this.flag = flag;
        this.expireBefore = expireBefore;
        this.version = version;
    }

    /**
     * Whether or not there is more atom to read.
     */
    public boolean hasNext() throws IOException
    {
        return nameDeserializer.hasNext();
    }

    /**
     * Whether or not some atom has been read but not processed (neither readNext() nor
     * skipNext() has been called for that atom) yet.
     */
    public boolean hasUnprocessed() throws IOException
    {
        return nameDeserializer.hasUnprocessed();
    }

    /**
     * Compare the provided prefix to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public int compareNextTo(ClusteringPrefix prefix) throws IOException
    {
        return nameDeserializer.compareNextTo(prefix);
    }

    /**
     * Returns the next atom.
     */
    public Atom readNext() throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //Composite name = nameDeserializer.readNext();
        //assert !name.isEmpty(); // This would imply hasNext() hasn't been called
        //int b = in.readUnsignedByte();
        //if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
        //    return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
        //else
        //    return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    }

    /**
     * Skips the next atom.
     */
    public void skipNext() throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //nameDeserializer.skipNext();
        //int b = in.readUnsignedByte();
        //if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
        //    type.rangeTombstoneSerializer().skipBody(in, version);
        //else
        //    type.columnSerializer().skipColumnBody(in, b);
    }
}
