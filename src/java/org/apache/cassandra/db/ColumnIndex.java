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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.Atom;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.atoms.AtomIterators;
import org.apache.cassandra.db.atoms.RangeTombstoneMarker;
import org.apache.cassandra.db.atoms.SimpleRangeTombstoneMarker;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnIndex
{
    public final List<IndexHelper.IndexInfo> columnsIndex;

    private static final ColumnIndex EMPTY = new ColumnIndex(Collections.<IndexHelper.IndexInfo>emptyList());

    private ColumnIndex(List<IndexHelper.IndexInfo> columnsIndex)
    {
        assert columnsIndex != null;

        this.columnsIndex = columnsIndex;
    }

    public static ColumnIndex writeAndBuildIndex(AtomIterator iterator, SequentialWriter output) throws IOException
    {
        assert !AtomIterators.isEmpty(iterator);

        Builder builder = new Builder(iterator, output);
        return builder.build();
    }

    @VisibleForTesting
    public static ColumnIndex nothing()
    {
        return EMPTY;
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    private static class Builder
    {
        private final AtomIterator iterator;
        private final SequentialWriter writer;

        private final ColumnIndex result;
        private final long indexOffset;
        private final long initialPosition;
        private long startPosition = -1;

        private int atomWritten;

        private ClusteringPrefix firstClustering;
        private final ReusableClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        public Builder(AtomIterator iterator,
                       SequentialWriter writer)
        {
            this.iterator = iterator;
            this.writer = writer;

            this.result = new ColumnIndex(new ArrayList<IndexHelper.IndexInfo>());
            this.indexOffset = partitionHeaderSize(iterator.partitionKey().getKey(), iterator.partitionLevelDeletion());
            this.initialPosition = writer.getFilePointer();
            this.lastClustering = new ReusableClusteringPrefix(iterator.metadata().clusteringColumns().size());
        }

        /**
         * Returns the number of bytes between the beginning of the row and the
         * first serialized column.
         */
        private static long partitionHeaderSize(ByteBuffer key, DeletionTime topLevelDeletion)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            // TODO fix constantSize when changing the nativeconststs.
            int keysize = key.remaining();
            return typeSizes.sizeof((short) keysize) + keysize          // Row key
                 + DeletionTime.serializer.serializedSize(topLevelDeletion, typeSizes);
        }

        private void writePartitionHeader(ByteBuffer key, DeletionTime topLevelDeletion) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(key, writer.stream);
            DeletionTime.serializer.serialize(topLevelDeletion, writer.stream);
        }

        public ColumnIndex build() throws IOException
        {
            writePartitionHeader(iterator.partitionKey().getKey(), iterator.partitionLevelDeletion());

            try (AtomIterator iter = iterator)
            {
                while (iter.hasNext())
                    add(iter.next());

                return close();
            }
        }

        private long currentPosition()
        {
            return writer.getFilePointer() - initialPosition;
        }

        private void addIndexBlock()
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstClustering.takeAlias(),
                                                                         lastClustering.takeAlias(),
                                                                         indexOffset + startPosition,
                                                                         currentPosition() - startPosition);
            result.columnsIndex.add(cIndexInfo);
            firstClustering = null;
        }

        private void add(Atom atom) throws IOException
        {
            ClusteringPrefix clustering = atom.clustering();

            if (firstClustering == null)
            {
                firstClustering = clustering.takeAlias();
                startPosition = currentPosition();
                if (openMarker != null)
                {
                    RangeTombstoneMarker marker = new SimpleRangeTombstoneMarker(firstClustering, true, openMarker);
                    iterator.metadata().layout().oldFormatAtomSerializer().serializeForSSTable(marker, writer.stream);
                    ++atomWritten;
                }
            }

            iterator.metadata().layout().oldFormatAtomSerializer().serializeForSSTable(atom, writer.stream);
            ++atomWritten;

            if (atom.kind() == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker)atom;
                openMarker = marker.isOpenMarker()
                           ? marker.delTime().takeAlias()
                           : null;
            }

            lastClustering.copy(clustering);

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                addIndexBlock();
        }

        private ColumnIndex close() throws IOException
        {
            writer.stream.writeShort(SSTableWriter.END_OF_ROW);

            // It's possible we add no atoms, just a top level deletion
            if (atomWritten == 0)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert result.columnsIndex.size() > 0;
            return result;
        }
    }
}
