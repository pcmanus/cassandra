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
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.SearchIterator;

public abstract class AbstractPartitionData implements Iterable<Row>, Partition
{
    protected final CFMetaData metadata;
    protected final DecoratedKey key;

    protected final DeletionInfo deletionInfo;
    protected final PartitionColumns columns;

    protected Row staticRow;

    protected int rows;

    private final AtomStats.Collector statsCollector = new AtomStats.Collector();

    // row 'i' clustering prefix is composed of the metadata.clusteringColumns.size() elements starting at 'clustering[rows * i]',
    // its timestamp is at 'timestamps[i]' and the row itself is at 'updates[i]'. The index 'i' in timestamps and updates
    // is used for the static row.
    protected ByteBuffer[] clusterings;
    protected long[] timestamps;
    protected final RowDataBlock data;

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    ByteBuffer[] clusterings,
                                    long[] timestamps,
                                    PartitionColumns columns,
                                    RowDataBlock data)
    {
        this.metadata = metadata;
        this.key = key;
        this.deletionInfo = deletionInfo;
        this.clusterings = clusterings;
        this.timestamps = timestamps;
        this.columns = columns;
        this.data = data;

        statsCollector.updateDeletionTime(deletionInfo.getTopLevelDeletion());
        Iterator<RangeTombstone> iter = deletionInfo.rangeIterator();
        while (iter.hasNext())
            statsCollector.updateDeletionTime(iter.next().data);
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    PartitionColumns columns,
                                    RowDataBlock data,
                                    int initialRowCapacity)
    {
        this(metadata,
             key,
             deletionInfo,
             new ByteBuffer[initialRowCapacity * metadata.clusteringColumns().size()],
             new long[initialRowCapacity],
             columns,
             data);
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionTime partitionDeletion,
                                    PartitionColumns columns,
                                    int initialRowCapacity)
    {
        this(metadata,
             key,
             new DeletionInfo(partitionDeletion.takeAlias()),
             columns,
             new RowDataBlock(columns.regulars, initialRowCapacity),
             initialRowCapacity);
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo.getTopLevelDeletion();
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public Row staticRow()
    {
        return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
    }

    public AtomStats stats()
    {
        return statsCollector.get();
    }

    public void addPartitionDeletion(DeletionTime deletionTime)
    {
        statsCollector.updateDeletionTime(deletionTime);
        deletionInfo.add(deletionTime);
    }

    public void addRangeTombstone(ClusteringPrefix from, ClusteringPrefix to, DeletionTime deletion)
    {
        addRangeTombstone(new RangeTombstone(from.takeAlias(), to.takeAlias(), deletion.takeAlias()));
    }

    public void addRangeTombstone(RangeTombstone range)
    {
        statsCollector.updateDeletionTime(range.data);
        deletionInfo.add(range, metadata.comparator);
    }

    public int rowCount()
    {
        return rows;
    }

    public boolean isEmpty()
    {
        return deletionInfo.isLive() && rows == 0 && staticRow == null;
    }

    public Iterator<Row> iterator()
    {
        return new RowIterator();
    }

    public SearchIterator<ClusteringPrefix, Row> searchIterator()
    {
        throw new UnsupportedOperationException();
    }

    public AtomIterator atomIterator(PartitionColumns columns, Slices slices, boolean reversed)
    {
        return slices.makeSliceIterator(seekableAtomIterator(columns, reversed));
    }

    protected SeekableAtomIterator seekableAtomIterator(PartitionColumns columns, boolean reversed)
    {
        return new AbstractSeekableIterator(this, columns, reversed)
        {
            private final RowIterator rowIterator = new RowIterator();
            private RowAndTombstoneMergeIterator mergeIterator = new RowAndTombstoneMergeIterator(metadata.comparator);

            protected Atom computeNext()
            {
                if (!mergeIterator.isSet())
                    mergeIterator.setTo(rowIterator, deletionInfo.rangeIterator());

                return mergeIterator.hasNext() ? mergeIterator.next() : endOfData();
            }

            public boolean seekTo(ClusteringPrefix from, ClusteringPrefix to)
            {
                rowIterator.seekTo(from);
                mergeIterator.setTo(rowIterator, deletionInfo.rangeIterator(from, EmptyClusteringPrefix.TOP));
                return mergeIterator.hasNext() && metadata.comparator.compare(mergeIterator.peek().clustering(), to) <= 0;
            }
        };
    }

    private class RowIterator extends AbstractIterator<Row>
    {
        private final AbstractReusableRow reusableRow = new AbstractReusableRow()
        {
            protected RowDataBlock data()
            {
                return data;
            }

            protected int row()
            {
                return row;
            }

            public ClusteringPrefix clustering()
            {
                return clustering;
            }

            public long timestamp()
            {
                return timestamps[row];
            }
        };

        private final ClusteringPrefix clustering = new AbstractClusteringPrefix()
        {
            final int size = metadata.clusteringColumns().size();

            public int size()
            {
                return size;
            }

            public ByteBuffer get(int i)
            {
                int base = row * size;
                return clusterings[base + i];
            }
        };

        private int row = -1;

        public Row computeNext()
        {
            return ++row >= rows ? endOfData() : reusableRow;
        }

        public void seekTo(ClusteringPrefix from)
        {
            binarySearch(row, rows, from);
            // Since we'll actually increment row first thing in computeNext, decrement it now in preparation
            --row;
        }

        /**
         * Simple binary search.
         * This sets row on either the search name if it's found, or on the "insertion point".
         */
        private void binarySearch(int fromIndex, int toIndex, ClusteringPrefix name)
        {
            int low = fromIndex;
            row = toIndex;
            int high = row - 1;
            int result = -1;
            while (low <= high)
            {
                row = (low + high) >> 1;
                if ((result = metadata.comparator.compare(name, clustering)) > 0)
                    low = row + 1;
                else if (result == 0)
                    return;
                else
                    high = row - 1;
            }
            if (result >= 0)
                row += 1;
        }
    }

    private static abstract class AbstractSeekableIterator extends AbstractAtomIterator implements SeekableAtomIterator
    {
        private AbstractSeekableIterator(AbstractPartitionData data, PartitionColumns columns, boolean isReverseOrder)
        {
            super(data.metadata, data.key, data.partitionLevelDeletion(), columns, data.staticRow(), isReverseOrder, data.stats());
        }
    }

    protected class Writer extends RowDataBlock.Writer
    {
        public Writer()
        {
            super(data);
        }

        public void writeClustering(ClusteringPrefix clustering)
        {
            assert clustering.eoc() == ClusteringPrefix.EOC.NONE;
            ensureCapacity(row);
            int base = row * metadata.clusteringColumns().size();
            for (int i = 0; i < clustering.size(); i++)
                clusterings[base + i] = clustering.get(i);
        }

        public void writeTimestamp(long timestamp)
        {
            ensureCapacity(row);
            timestamps[row] = timestamp;
            statsCollector.updateTimestamp(timestamp);
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            statsCollector.updateTimestamp(timestamp);
            statsCollector.updateLocalDeletionTime(localDeletionTime);
            statsCollector.updateTTL(ttl);

            super.writeCell(column, isCounter, value, timestamp, localDeletionTime, ttl, path);
        }

        @Override
        public void writeComplexDeletion(ColumnDefinition c, DeletionTime complexDeletion)
        {
            statsCollector.updateDeletionTime(complexDeletion);

            super.writeComplexDeletion(c, complexDeletion);
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            ++rows;
        }

        private void ensureCapacity(int rowToSet)
        {
            int originalCapacity = timestamps.length;
            if (rowToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);

            int clusteringSize = metadata.clusteringColumns().size();

            clusterings = Arrays.copyOf(clusterings, newCapacity * clusteringSize);
            timestamps = Arrays.copyOf(timestamps, newCapacity);

            Arrays.fill(timestamps, originalCapacity, newCapacity, Cells.NO_TIMESTAMP);
        }
    }

    protected class RangeTombstoneCollector implements RangeTombstoneMarker.Writer
    {
        private ClusteringPrefix open;
        private DeletionTime data;

        public void writeMarker(ClusteringPrefix clustering, boolean isOpenMarker, DeletionTime delTime)
        {
            clustering = clustering.takeAlias();
            if (isOpenMarker)
            {
                if (open != null)
                    addRangeTombstone(open, clustering, data);
                open = clustering;
                data = delTime.takeAlias();
            }
            else
            {
                assert open != null;
                addRangeTombstone(open, clustering, data);
            }
        }

        private void addRangeTombstone(ClusteringPrefix min, ClusteringPrefix max, DeletionTime dt)
        {
            AbstractPartitionData.this.addRangeTombstone(min, max, dt);
        }
    }
}
