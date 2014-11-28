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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

/**
 *  A Cell Iterator over SSTable
 */
public class SSTableIterator implements SeekableAtomIterator
{
    private final SSTableReader sstable;
    private final DecoratedKey key;
    private final DeletionTime partitionLevelDeletion;
    private final PartitionColumns columns;

    private final Row staticRow;
    private final Reader reader;

    public SSTableIterator(SSTableReader sstable, DecoratedKey key, PartitionColumns columns)
    {
        this(sstable, null, key, columns, sstable.getPosition(key, SSTableReader.Operator.EQ));
    }

    public SSTableIterator(SSTableReader sstable,
                           FileDataInput file,
                           DecoratedKey key,
                           PartitionColumns columns,
                           RowIndexEntry indexEntry)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columns;

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            try
            {
                boolean shouldCloseFile = file == null;
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     and we need to read the partition deletion time.
                //   - we're querying static columns.
                if (indexEntry.isIndexed() && columns.statics.isEmpty())
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                }
                else
                {
                    // Not indexed, set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.position);
                    else
                        file.seek(indexEntry.position);

                    ByteBufferUtil.readWithShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
                    this.staticRow = columns.statics.isEmpty() ? Rows.EMPTY_STATIC_ROW : readStaticRow(file);
                }

                this.reader = indexEntry.isIndexed()
                            ? new IndexedReader(indexEntry, file, shouldCloseFile)
                            : new SimpleReader(file, shouldCloseFile);
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, file.getPath());
            }
        }
    }

    private Row readStaticRow(FileDataInput file)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public AtomStats stats()
    {
        // TODO: we should start collecting the minTimestamp and the minTTL
        return new AtomStats(sstable.getMinTimestamp(), Cells.NO_DELETION_TIME, Cells.NO_TTL);
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public boolean hasNext()
    {
        try
        {
            return reader != null && reader.hasNext();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public Atom next()
    {
        try
        {
            return reader.next();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public boolean seekTo(ClusteringPrefix from, ClusteringPrefix to)
    {
        try
        {
            return reader.seekTo(from, to);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        if (reader != null)
        {
            try
            {
                reader.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }
    }

    private abstract class Reader
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        protected AtomDeserializer deserializer;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;
        }

        protected void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = new AtomDeserializer(sstable.metadata,
                                                file,
                                                LegacyLayout.Flag.LOCAL,
                                                Integer.MIN_VALUE,
                                                sstable.descriptor.version,
                                                columns.regulars);
        }

        public boolean hasNext() throws IOException
        {
            // We currently assume that seekTo has been called before any call to this method. If
            // that wasn't the case we could simply seek to the beginning of the partition in
            // IndexedReader, but as neither Slices nor Names queries will use this, so we don't
            // bother for now.
            assert deserializer != null;
            return deserializer.hasNext();
        }

        public Atom next() throws IOException
        {
            return deserializer.readNext();
        }

        public abstract boolean seekTo(ClusteringPrefix from, ClusteringPrefix to) throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    private class SimpleReader extends Reader
    {
        private SimpleReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            assert file != null;
            createDeserializer();
        }

        public boolean seekTo(ClusteringPrefix from, ClusteringPrefix to) throws IOException
        {
            // Skip entries until we're set where we want to be. It's possible that
            // we go over to.
            while (deserializer.hasNext() && deserializer.compareNextTo(from) < 0)
                deserializer.skipNext();

            // It's a successful seek only if the next atom is actually smaller or equal to 'to'
            return deserializer.hasNext() && deserializer.compareNextTo(to) <= 0;
        }
    }

    private class IndexedReader extends Reader
    {
        private final RowIndexEntry indexEntry;
        private int indexIdx = -1;

        private IndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            this.indexEntry = indexEntry;

            if (file != null)
                createDeserializer();
        }

        public boolean seekTo(ClusteringPrefix from, ClusteringPrefix to) throws IOException
        {
            List<IndexHelper.IndexInfo> indexes = indexEntry.columnsIndex();

            // if we're already past the biggest row in the sstable, we're done
            if (indexIdx >= indexes.size())
                return false;

            // Find index block corresponding to c
            int newIdx = IndexHelper.indexFor(from, indexes, sstable.metadata.comparator, false, indexIdx);

            // Set file/deserializer to that block
            if (newIdx != indexIdx)
            {
                indexIdx = newIdx;

                if (indexIdx >= indexes.size())
                    return false;

                // The search above gave us that indexes[indexIdx - 1].lastName <= from <= indexes[indexIdx].lastName.
                // It's still possible that to < index[indexIdx].firstName, in which case we know we'll have no atom
                // to return
                IndexHelper.IndexInfo info = indexes.get(indexIdx);
                if (sstable.metadata.comparator.compare(to, info.firstName) < 0)
                    return false;

                long positionToSeek = indexEntry.position + info.offset;

                // This may be the first time we're actually looking into the file
                if (file == null)
                {
                    file = sstable.getFileDataInput(positionToSeek);
                    createDeserializer();
                }
                else
                {
                    file.seek(positionToSeek);
                }

                // Clear the state in the deserializer if it's not clear already
                if (deserializer.hasUnprocessed())
                    deserializer.skipNext();
            }

            // Skip entries until we're set where we want to be (note that, due to the logic above, we know
            // that we'll eventually reach a matching atom, we don't have to check if there is data and if
            // we don't go after 'to')
            while (deserializer.compareNextTo(from) < 0)
                deserializer.skipNext();

            return true;
        }
    }
}
