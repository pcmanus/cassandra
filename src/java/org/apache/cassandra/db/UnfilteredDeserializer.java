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

import java.io.IOException;
import java.io.IOError;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.net.MessagingService;

/**
 * Helper class to deserialize Unfiltered object from disk efficiently.
 *
 * More precisely, this class is used by the low-level reader to ensure
 * we don't do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public abstract class UnfilteredDeserializer
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredDeserializer.class);

    protected final CFMetaData metadata;
    protected final DataInputPlus in;
    protected final SerializationHelper helper;

    protected UnfilteredDeserializer(CFMetaData metadata,
                                     DataInputPlus in,
                                     SerializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static UnfilteredDeserializer create(CFMetaData metadata,
                                                DataInputPlus in,
                                                SerializationHeader header,
                                                SerializationHelper helper,
                                                DeletionTime partitionDeletion,
                                                boolean readAllAsDynamic)
    {
        return new CurrentDeserializer(metadata, in, header, helper);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public abstract int compareNextTo(ClusteringBound bound) throws IOException;

    /**
     * Returns whether the next atom is a row or not.
     */
    public abstract boolean nextIsRow() throws IOException;

    /**
     * Returns whether the next atom is the static row or not.
     */
    public abstract boolean nextIsStatic() throws IOException;

    /**
     * Returns the next atom.
     */
    public abstract Unfiltered readNext() throws IOException;

    /**
     * Clears any state in this deserializer.
     */
    public abstract void clearState() throws IOException;

    /**
     * Skips the next atom.
     */
    public abstract void skipNext() throws IOException;

    private static class CurrentDeserializer extends UnfilteredDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
        private int nextExtendedFlags;
        private boolean isReady;
        private boolean isDone;

        private final Row.Builder builder;

        private CurrentDeserializer(CFMetaData metadata,
                                    DataInputPlus in,
                                    SerializationHeader header,
                                    SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
            this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
            this.builder = BTreeRow.sortedBuilder();
        }

        public boolean hasNext() throws IOException
        {
            if (isReady)
                return true;

            prepareNext();
            return !isDone;
        }

        private void prepareNext() throws IOException
        {
            if (isDone)
                return;

            nextFlags = in.readUnsignedByte();
            if (UnfilteredSerializer.isEndOfPartition(nextFlags))
            {
                isDone = true;
                isReady = false;
                return;
            }

            nextExtendedFlags = UnfilteredSerializer.readExtendedFlags(in, nextFlags);

            clusteringDeserializer.prepare(nextFlags, nextExtendedFlags);
            isReady = true;
        }

        public int compareNextTo(ClusteringBound bound) throws IOException
        {
            if (!isReady)
                prepareNext();

            assert !isDone;

            return clusteringDeserializer.compareNextTo(bound);
        }

        public boolean nextIsRow() throws IOException
        {
            if (!isReady)
                prepareNext();

            return UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.ROW;
        }

        public boolean nextIsStatic() throws IOException
        {
            // This exists only for the sake of the OldFormatDeserializer
            throw new UnsupportedOperationException();
        }

        public Unfiltered readNext() throws IOException
        {
            isReady = false;
            if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                ClusteringBoundOrBoundary bound = clusteringDeserializer.deserializeNextBound();
                return UnfilteredSerializer.serializer.deserializeMarkerBody(in, header, bound);
            }
            else
            {
                builder.newRow(clusteringDeserializer.deserializeNextClustering());
                return UnfilteredSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, nextExtendedFlags, builder);
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            clusteringDeserializer.skipNext();
            if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                UnfilteredSerializer.serializer.skipMarkerBody(in);
            }
            else
            {
                UnfilteredSerializer.serializer.skipRowBody(in);
            }
        }

        public void clearState()
        {
            isReady = false;
            isDone = false;
        }
    }
}
