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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract common class for all non-thread safe Partition implementations.
 */
public abstract class AbstractPartitionData implements Partition
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionData.class);

    protected final CFMetaData metadata;
    protected final DecoratedKey key;

    protected final DeletionInfo deletionInfo;
    protected final PartitionColumns columns;

    // The time (in seconds) at which the partition has been created.
    protected final int createdAtInSec;

    protected Row staticRow;

    protected int rows;

    // The values for the clustering columns of the rows contained in this partition object. If
    // clusteringSize is the size of the clustering comparator for this table, clusterings has size
    // clusteringSize * rows where rows is the number of rows stored, and row i has it's clustering
    // column values at indexes [clusteringSize * i, clusteringSize * (i + 1)).
    protected ByteBuffer[] clusterings;

    // The partition key column liveness infos for the rows of this partition (row i has its liveness info at index i).
    protected final LivenessInfoArray livenessInfos;
    // The row deletion for the rows of this partition (row i has its row deletion at index i).
    protected final DeletionTimeArray deletions;

    // The row data (cells data + complex deletions for complex columns) for the rows contained in this partition.
    protected final RowDataBlock data;

    // The max live timestamps for the rows of this parition (row i has its max live timestamp at index i).
    // Please note that the values of this array are only valid at createdAtInSec. Otherwise, the value for the max
    // live timestamp needs to be recomputed since new values may have expired.
    protected long[] maxLiveTimestamps;

    // Stats over the atoms stored in this partition.
    private final AtomStats.Collector statsCollector = new AtomStats.Collector();

    // The maximum timestamp for any data contained in this partition.
    protected long maxTimestamp = Long.MIN_VALUE;

    private AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    ByteBuffer[] clusterings,
                                    LivenessInfoArray livenessInfos,
                                    long[] maxLiveTimestamps,
                                    DeletionTimeArray deletions,
                                    PartitionColumns columns,
                                    RowDataBlock data,
                                    int createdAtInSec)
    {
        this.metadata = metadata;
        this.key = key;
        this.deletionInfo = deletionInfo;
        this.clusterings = clusterings;
        this.livenessInfos = livenessInfos;
        this.maxLiveTimestamps = maxLiveTimestamps;
        this.deletions = deletions;
        this.columns = columns;
        this.data = data;
        this.createdAtInSec = createdAtInSec;

        collectStats(deletionInfo.getPartitionDeletion());
        Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
        while (iter.hasNext())
            collectStats(iter.next().deletionTime());
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    PartitionColumns columns,
                                    RowDataBlock data,
                                    int initialRowCapacity,
                                    int nowInSec)
    {
        this(metadata,
             key,
             deletionInfo,
             new ByteBuffer[initialRowCapacity * metadata.clusteringColumns().size()],
             new LivenessInfoArray(initialRowCapacity),
             newMaxLiveTimestmapArray(initialRowCapacity),
             new DeletionTimeArray(initialRowCapacity),
             columns,
             data,
             nowInSec);
    }

    private static long[] newMaxLiveTimestmapArray(int length)
    {
        long[] a = new long[length];
        Arrays.fill(a, LivenessInfo.NO_TIMESTAMP);
        return a;
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionTime partitionDeletion,
                                    PartitionColumns columns,
                                    int initialRowCapacity,
                                    boolean sortable,
                                    int nowInSec)
    {
        this(metadata,
             key,
             new DeletionInfo(partitionDeletion.takeAlias()),
             columns,
             new RowDataBlock(columns.regulars, initialRowCapacity, sortable),
             initialRowCapacity,
             nowInSec);
    }

    private void collectStats(DeletionTime dt)
    {
        statsCollector.updateDeletionTime(dt);
        maxTimestamp = Math.max(maxTimestamp, dt.markedForDeleteAt());
    }

    private void collectStats(LivenessInfo info)
    {
        statsCollector.updateTimestamp(info.timestamp());
        statsCollector.updateTTL(info.ttl());
        statsCollector.updateLocalDeletionTime(info.localDeletionTime());
        maxTimestamp = Math.max(maxTimestamp, info.timestamp());
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
        return deletionInfo.getPartitionDeletion();
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

    public int nowInSec()
    {
        return createdAtInSec;
    }

    public void addPartitionDeletion(DeletionTime deletionTime)
    {
        collectStats(deletionTime);
        deletionInfo.add(deletionTime);
    }

    public void addRangeTombstone(Slice deletedSlice, DeletionTime deletion)
    {
        addRangeTombstone(new RangeTombstone(deletedSlice, deletion.takeAlias()));
    }

    public void addRangeTombstone(RangeTombstone range)
    {
        collectStats(range.deletionTime());
        deletionInfo.add(range, metadata.comparator);
    }

    /**
     * Swap row i and j.
     *
     * This is only used when we need to reorder rows because those were not added in clustering order,
     * which happens in {@link PartitionUpdate#sort} and {@link ArrayBackedPartition#create}. This method
     * is public only because {@code PartitionUpdate} needs to implement {@link Sorting.Sortable}, but
     * it should really only be used by subclasses (and with care) in practice.
     */
    public void swap(int i, int j)
    {
        int cs = metadata.clusteringColumns().size();
        for (int k = 0; k < cs; k++)
        {
            ByteBuffer tmp = clusterings[j * cs + k];
            clusterings[j * cs + k] = clusterings[i * cs + k];
            clusterings[i * cs + k] = tmp;
        }

        livenessInfos.swap(i, j);
        deletions.swap(i, j);
        data.swap(i, j);

        long tmp = maxLiveTimestamps[j];
        maxLiveTimestamps[j] = maxLiveTimestamps[i];
        maxLiveTimestamps[i] = tmp;
    }

    public int rowCount()
    {
        return rows;
    }

    public boolean isEmpty()
    {
        return deletionInfo.isLive() && rows == 0 && staticRow == null;
    }

    protected void clear()
    {
        rows = 0;
        Arrays.fill(clusterings, null);
        livenessInfos.clear();
        deletions.clear();
        Arrays.fill(maxLiveTimestamps, LivenessInfo.NO_TIMESTAMP);
        data.clear();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = metadata();
        sb.append(String.format("Partition[%s.%s] key=%s columns=%s\n",
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(partitionKey().getKey()),
                    columns()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("-----\n").append(staticRow().toString(metadata));

        // We use createRowIterator() directly instead of iterator() because that avoids
        // sorting for PartitionUpdate (which inherit this method) and that is useful because
        //  1) it can help with debugging and 2) we can't write after sorting but we want to
        // be able to print an update while we build it (again for debugging)
        Iterator<Row> iterator = createRowIterator(null, createdAtInSec, false);
        while (iterator.hasNext())
            sb.append("\n-----\n").append(iterator.next().toString(metadata, true));

        sb.append("\n-----\n");
        return sb.toString();
    }

    public Iterator<Row> iterator(int nowInSec)
    {
        return createRowIterator(null, nowInSec, false);
    }

    public SearchIterator<Clustering, Row> searchIterator(final PartitionColumns columns, boolean reversed, int nowInSec)
    {
        final RowIterator iter = createRowIterator(columns, nowInSec, reversed);
        return new SearchIterator<Clustering, Row>()
        {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next(Clustering key)
            {
                if (key == Clustering.STATIC_CLUSTERING)
                {
                    if (columns.statics.isEmpty() || staticRow().isEmpty())
                        return Rows.EMPTY_STATIC_ROW;

                    return FilteringRow.columnsFilteringRow(columns.statics).setTo(staticRow());
                }

                return iter.seekTo(key) ? iter.next() : null;
            }
        };
    }

    public AtomIterator toAtomIterator()
    {
        return atomIterator(columns(), Slices.ALL, false, nowInSec());
    }

    public AtomIterator atomIterator(PartitionColumns columns, Slices slices, boolean reversed, int nowInSec)
    {
        return slices.makeSliceIterator(sliceableAtomIterator(columns, reversed, nowInSec));
    }

    protected SliceableAtomIterator sliceableAtomIterator(final PartitionColumns columns, final boolean reversed, final int nowInSec)
    {
        return new AbstractSliceableIterator(this, columns, nowInSec, reversed)
        {
            private final RowIterator rowIterator = createRowIterator(columns, nowInSec, reversed);
            private RowAndTombstoneMergeIterator mergeIterator = new RowAndTombstoneMergeIterator(metadata.comparator, reversed);

            protected Atom computeNext()
            {
                if (!mergeIterator.isSet())
                    mergeIterator.setTo(rowIterator, deletionInfo.rangeIterator(reversed));

                return mergeIterator.hasNext() ? mergeIterator.next() : endOfData();
            }

            public Iterator<Atom> slice(Slice slice)
            {
                return mergeIterator.setTo(rowIterator.slice(slice), deletionInfo.rangeIterator(slice, reversed));
            }
        };
    }

    private RowIterator createRowIterator(PartitionColumns columns, int nowInSec, boolean reversed)
    {
        return reversed ? new ReverseRowIterator(columns, nowInSec) : new ForwardRowIterator(columns, nowInSec);
    }

    /**
     * An iterator over the rows of this partition that reuse the same row object.
     */
    private abstract class RowIterator extends UnmodifiableIterator<Row>
    {
        protected final InternalReusableClustering clustering = new InternalReusableClustering();
        protected final InternalReusableRow reusableRow;
        protected final FilteringRow filter;

        protected int next;

        protected RowIterator(final PartitionColumns columns, int nowInSec)
        {
            this.reusableRow = new InternalReusableRow(clustering, nowInSec);
            this.filter = columns == null ? null : FilteringRow.columnsFilteringRow(columns);
        }

        /*
         * Move the iterator so that row {@code name} is returned next by {@code next} if that
         * row exists. Otherwise the first row sorting after {@code name} will be returned.
         * Returns whether {@code name} was found or not.
         */
        public abstract boolean seekTo(Clustering name);

        public abstract Iterator<Row> slice(Slice slice);

        protected Row setRowTo(int row)
        {
            reusableRow.setTo(row);
            return filter == null ? reusableRow : filter.setTo(reusableRow);
        }

        /**
         * Simple binary search.
         */
        protected int binarySearch(ClusteringPrefix name, int fromIndex, int toIndex)
        {
            int low = fromIndex;
            int mid = toIndex;
            int high = mid - 1;
            int result = -1;
            while (low <= high)
            {
                mid = (low + high) >> 1;
                if ((result = metadata.comparator.compare(name, clustering.setTo(mid))) > 0)
                    low = mid + 1;
                else if (result == 0)
                    return mid;
                else
                    high = mid - 1;
            }
            return -mid - (result < 0 ? 1 : 2);
        }
    }

    private class ForwardRowIterator extends RowIterator
    {
        private ForwardRowIterator(PartitionColumns columns, int nowInSec)
        {
            super(columns, nowInSec);
            this.next = 0;
        }

        public boolean hasNext()
        {
            return next < rows;
        }

        public Row next()
        {
            return setRowTo(next++);
        }

        public boolean seekTo(Clustering name)
        {
            if (next >= rows)
                return false;

            int idx = binarySearch(name, next, rows);
            next = idx >= 0 ? idx : -idx - 1;
            return idx >= 0;
        }

        public Iterator<Row> slice(Slice slice)
        {
            int sidx = binarySearch(slice.start(), next, rows);
            final int start = sidx >= 0 ? sidx : -sidx - 1;
            if (start >= rows)
                return Collections.emptyIterator();

            int eidx = binarySearch(slice.end(), start, rows);
            // The insertion point is the first element greater than slice.end(), so we want the previous index
            final int end = eidx >= 0 ? eidx : -eidx - 2;

            // Remember the end to speed up potential further slice search
            next = end;

            if (start > end)
                return Collections.emptyIterator();

            return new AbstractIterator<Row>()
            {
                private int i = start;

                protected Row computeNext()
                {
                    if (i >= rows || i > end)
                        return endOfData();

                    return setRowTo(i++);
                }
            };
        }
    }

    private class ReverseRowIterator extends RowIterator
    {
        private ReverseRowIterator(PartitionColumns columns, int nowInSec)
        {
            super(columns, nowInSec);
            this.next = rows - 1;
        }

        public boolean hasNext()
        {
            return next >= 0;
        }

        public Row next()
        {
            return setRowTo(next--);
        }

        public boolean seekTo(Clustering name)
        {
            // We only use that method with forward iterators.
            throw new UnsupportedOperationException();
        }

        public Iterator<Row> slice(Slice slice)
        {
            int sidx = binarySearch(slice.end(), 0, next + 1);
            // The insertion point is the first element greater than slice.end(), so we want the previous index
            final int start = sidx >= 0 ? sidx : -sidx - 2;
            if (start < 0)
                return Collections.emptyIterator();

            int eidx = binarySearch(slice.start(), 0, start + 1);
            final int end = eidx >= 0 ? eidx : -eidx - 1;

            // Remember the end to speed up potential further slice search
            next = end;

            if (start < end)
                return Collections.emptyIterator();

            return new AbstractIterator<Row>()
            {
                private int i = start;

                protected Row computeNext()
                {
                    if (i < 0 || i < end)
                        return endOfData();

                    return setRowTo(i--);
                }
            };
        }
    }

    /**
     * A reusable view over the clustering of this partition.
     */
    protected class InternalReusableClustering extends Clustering
    {
        final int size = metadata.clusteringColumns().size();
        private int base;

        public int size()
        {
            return size;
        }

        public Clustering setTo(int row)
        {
            base = row * size;
            return this;
        }

        public ByteBuffer get(int i)
        {
            return clusterings[base + i];
        }
    };

    /**
     * A reusable view over the rows of this partition.
     */
    protected class InternalReusableRow extends AbstractReusableRow
    {
        private final LivenessInfoArray.Cursor liveness = new LivenessInfoArray.Cursor();
        private final DeletionTimeArray.Cursor deletion = new DeletionTimeArray.Cursor();
        private final InternalReusableClustering clustering;
        private final int nowInSec;

        private int row;

        public InternalReusableRow(int nowInSec)
        {
            this(new InternalReusableClustering(), nowInSec);
        }

        public InternalReusableRow(InternalReusableClustering clustering, int nowInSec)
        {
            this.clustering = clustering;
            this.nowInSec = nowInSec;
        }

        protected RowDataBlock data()
        {
            return data;
        }

        public Row setTo(int row)
        {
            this.clustering.setTo(row);
            this.liveness.setTo(livenessInfos, row);
            this.deletion.setTo(deletions, row);
            this.row = row;
            return this;
        }

        protected int row()
        {
            return row;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        public LivenessInfo partitionKeyLivenessInfo()
        {
            return liveness;
        }

        public long maxLiveTimestamp()
        {
            // If the time of the query used is the one of the creation of this partition, then
            // the maxLiveTimestamp information is valid. Otherwise, it needs to be recomputed.
            return nowInSec == createdAtInSec
                 ? maxLiveTimestamps[row]
                 : Rows.computeMaxLiveTimestamp(this);
        }

        public DeletionTime deletion()
        {
            return deletion;
        }

        public int nowInSec()
        {
            return nowInSec;
        }
    };

    private static abstract class AbstractSliceableIterator extends AbstractAtomIterator implements SliceableAtomIterator
    {
        private AbstractSliceableIterator(AbstractPartitionData data, PartitionColumns columns, int nowInSec, boolean isReverseOrder)
        {
            super(data.metadata, data.key, data.partitionLevelDeletion(), columns, data.staticRow(), isReverseOrder, data.stats(), nowInSec);
        }
    }

    /**
     * A row writer to add rows to this partition.
     */
    protected class Writer extends RowDataBlock.Writer
    {
        private int clusteringBase;

        private int simpleColumnsSetInRow;
        private final Set<ColumnDefinition> complexColumnsSetInRow = new HashSet<>();

        public Writer()
        {
            super(data);
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            ensureCapacity(row);
            clusterings[clusteringBase++] = value;
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            ensureCapacity(row);
            livenessInfos.set(row, info);
            collectStats(info);
            updateMaxLiveTimestamp(info.timestamp());
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            ensureCapacity(row);
            if (!deletion.isLive())
                deletions.set(row, deletion);

            collectStats(deletion);
        }

        public void writeMaxLiveTimestamp(long maxLiveTimestamp)
        {
            ensureCapacity(row);
            updateMaxLiveTimestamp(maxLiveTimestamp);
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            ensureCapacity(row);
            collectStats(info);

            // When we're writting, we might not have had an accurate value for maxLiveTimestamp in the writeRowInfos() call, so update it.
            // (if we did have the proper value, this is just a no-op)
            if (info.isLive(createdAtInSec))
                updateMaxLiveTimestamp(info.timestamp());

            if (column.isComplex())
                complexColumnsSetInRow.add(column);
            else
                ++simpleColumnsSetInRow;

            super.writeCell(column, isCounter, value, info, path);
        }

        private void updateMaxLiveTimestamp(long timestamp)
        {
            if (timestamp > maxLiveTimestamps[row])
                maxLiveTimestamps[row] = timestamp;
        }

        @Override
        public void writeComplexDeletion(ColumnDefinition c, DeletionTime complexDeletion)
        {
            ensureCapacity(row);
            collectStats(complexDeletion);

            super.writeComplexDeletion(c, complexDeletion);
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            ++rows;

            statsCollector.updateColumnSetPerRow(simpleColumnsSetInRow + complexColumnsSetInRow.size());

            simpleColumnsSetInRow = 0;
            complexColumnsSetInRow.clear();
        }

        public int currentRow()
        {
            return row;
        }

        private void ensureCapacity(int rowToSet)
        {
            int originalCapacity = livenessInfos.size();
            if (rowToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);

            int clusteringSize = metadata.clusteringColumns().size();

            clusterings = Arrays.copyOf(clusterings, newCapacity * clusteringSize);

            livenessInfos.resize(newCapacity);
            deletions.resize(newCapacity);

            maxLiveTimestamps = Arrays.copyOf(maxLiveTimestamps, newCapacity);
            Arrays.fill(maxLiveTimestamps, originalCapacity, newCapacity, LivenessInfo.NO_TIMESTAMP);
        }

        @Override
        public Writer reset()
        {
            super.reset();
            clusteringBase = 0;
            simpleColumnsSetInRow = 0;
            complexColumnsSetInRow.clear();
            return this;
        }
    }

    /**
     * A range tombstone marker writer to add range tombstone markers to this partition.
     */
    protected class RangeTombstoneCollector implements RangeTombstoneMarker.Writer
    {
        private final ByteBuffer[] nextValues = new ByteBuffer[metadata().comparator.size()];
        private int size;
        private Slice.Bound.Kind nextKind;
        private DeletionTime nextDeletion;

        private Slice.Bound start;

        public RangeTombstoneCollector()
        {
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            nextValues[size++] = value;
        }

        public void writeBoundKind(Slice.Bound.Kind kind)
        {
            nextKind = kind;
        }

        public void writeMarkerDeletion(DeletionTime deletion)
        {
            if (nextKind.isStart())
                nextDeletion = deletion.takeAlias();
            else
                assert nextDeletion.equals(deletion);
        }

        public void endOfMarker()
        {
            if (nextKind.isStart())
            {
                start = Slice.Bound.create(nextKind, Arrays.copyOfRange(nextValues, 0, size));
            }
            else
            {
                Slice.Bound end = Slice.Bound.create(nextKind, Arrays.copyOfRange(nextValues, 0, size));
                addRangeTombstone(Slice.make(start, end), nextDeletion);
                reset();
            }
            Arrays.fill(nextValues, null);
            size = 0;
        }

        /**
         * Whether there is a currently open marker in the collector.
         */
        public boolean hasOpenMarker()
        {
            return start != null;
        }

        /**
         * Provided the collector has an open marker, close that marker using the provided bound.
         */
        public void closeOpenMarker(Slice.Bound bound)
        {
            assert hasOpenMarker();
            addRangeTombstone(Slice.make(start, bound), nextDeletion);
            reset();
        }

        private void addRangeTombstone(Slice deletionSlice, DeletionTime dt)
        {
            AbstractPartitionData.this.addRangeTombstone(deletionSlice, dt);
        }

        public void reset()
        {
            start = null;
            nextDeletion = null;
            size = 0;
            nextKind = null;
        }
    }
}
