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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Static methods to work with atom iterators.
 */
public abstract class AtomIterators
{
    private AtomIterators() {}

    public interface MergeListener
    {
        public void onMergingRows(ClusteringPrefix clustering, long mergedTimestamp, Row[] versions);
        public void onMergedColumns(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTimeArray versions);
        public void onMergedCells(Cell mergedCell, Cell[] versions);
        public void onRowDone();

        public void onMergedRangeTombstoneMarkers(ClusteringPrefix prefix, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions);

        public void close();
    }

    public abstract class AbstractMergeListener
    {
        protected AbstractMergeListener() {}

        public void onMergingRows(ClusteringPrefix clustering, long mergedTimestamp, Row[] versions) {}
        public void onMergedColumns(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTimeArray versions) {}
        public void onMergedCells(Cell mergedCell, Cell[] versions) {}
        public void onRowDone() {}

        public void onMergedRangeTombstoneMarkers(ClusteringPrefix prefix, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions) {}
    }

    /**
     * Returns whether the provided iterator has no data (including no deletion data).
     */
    public static boolean isEmpty(AtomIterator iterator)
    {
        return iterator.partitionLevelDeletion().isLive() && !iterator.hasNext() && iterator.staticRow().isEmpty();
    }

    /**
     * Returns a iterator that only returns rows with only live content.
     *
     * This is mainly used in the CQL layer when we know we don't care about deletion
     * infos (and since an AtomIterator cannot shadow it's own data, we know everyting
     * returned isn't shadowed by a tombstone).
     */
    public static RowIterator asRowIterator(AtomIterator iter, int nowInSec)
    {
        return new RowIteratorFromAtomIterator(iter, nowInSec);
    }

    /**
     * Returns an iterator that is the result of merging other iterators.
     */
    public static AtomIterator merge(List<AtomIterator> iterators, int nowInSec)
    {
        throw new UnsupportedOperationException();
        //assert !iterators.isEmpty();
        //return iterators.size() == 1 ? iterators.get(0) : new AtomMergeIterator(iterators, nowInSec);
    }

    /**
     * Returns an iterator that is the result of merging other iterators, and using
     * specific MergeListener.
     *
     * Note that this method assumes that there is at least 2 iterators to merge.
     */
    public static AtomIterator merge(List<AtomIterator> iterators, int nowInSec, MergeListener mergeListener)
    {
        throw new UnsupportedOperationException();
        //return new AtomMergeIterator(iterators, nowInSec, mergeListener);
    }

    /**
     * Returns an empty atom iterator for a given partition.
     */
    public static AtomIterator emptyIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final boolean isReverseOrder)
    {
        return new AtomIterator()
        {
            public CFMetaData metadata()
            {
                return cfm;
            }

            public boolean isReverseOrder()
            {
                return isReverseOrder;
            }

            public PartitionColumns columns()
            {
                return PartitionColumns.NONE;
            }

            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return DeletionTime.LIVE;
            }

            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }

            public AtomStats stats()
            {
                return AtomStats.NO_STATS;
            }

            public boolean hasNext()
            {
                return false;
            }

            public Atom next()
            {
                throw new NoSuchElementException();
            }

            public void remove()
            {
            }

            public void close()
            {
            }
        };
    }

    public static void digest(AtomIterator iterator, MessageDigest digest)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an iterator that concatenate two atom iterators.
     * This method assumes that both iterator are from the same partition and that the atom from
     * {@code iter2} come after the ones of {@code iter1} (that is, that concatenating the iterator
     * make sense).
     */
    public static AtomIterator concat(final AtomIterator iter1, final AtomIterator iter2)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static AtomIterator filterNulls(AtomIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static AtomIterator cloningIterator(AtomIterator iterator, AbstractAllocator allocator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static PartitionUpdate toUpdate(AtomIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static AtomIterator withValidation(AtomIterator iterator, String filename)
    {
        // TODO
        throw new UnsupportedOperationException();

        //try
        //{
        //    Atom atom = super.next();
        //    if (validateColumns)
        //        Atoms.validate(metadata, atom);
        //    return atom;
        //}
        //catch (MarshalException me)
        //{
        //    throw new CorruptSSTableException(me, filename);
        //}
    }

    /**
     * A wrapper over MergeIterator to implement the AtomIterator interface.
     *
     * NOTE: MergeIterator has to look-ahead in the merged iterators during hasNext, which means that
     * not only next() is destructive of the previously returned element but hasNext() is too. That
     * being said, it's not really harder to consider that both method are always destructive so we
     * live with it.
     */
    //private static class AtomMergeIterator extends AbstractIterator<Atom> implements AtomIterator
    //{
    //    private final CFMetaData metadata;
    //    private final DecoratedKey partitionKey;
    //    private final DeletionTime partitionLevelDeletion;
    //    private final IMergeIterator<Atom, Atom> mergeIterator;
    //    private final MergeListener listener;
    //    private final Columns columns;
    //    private final Columns staticColumns;
    //    private final Row staticRow;
    //    private final boolean isReverseOrder;

    //    private AtomMergeIterator(List<AtomIterator> iterators, int nowInSec, MergeListener listener)
    //    {
    //        assert iterators.size() > 1;

    //        // TODO: we could assert all iterators are on the same table, key, columns and order.
    //        AtomIterator first = iterators.get(0);

    //        this.metadata = first.metadata();
    //        this.partitionKey = first.partitionKey();
    //        this.isReverseOrder = first.isReverseOrder();
    //        // TODO: not sure that's actually ok
    //        this.columns = first.columns();
    //        this.staticColumns = first.staticColumns();
    //        this.partitionLevelDeletion = collectPartitionLevelDeletion(iterators);
    //        this.mergeIterator = MergeIterator.get(iterators,
    //                                               metadata.comparator.atomComparator,
    //                                               new MergeReducer(nowInSec, iterators.size(), first.columns(), listener));
    //        this.listener = listener;
    //        this.staticRow = mergeStaticRows(iterators, nowInSec, listener);
    //    }

    //    private AtomMergeIterator(List<AtomIterator> iterators, int nowInSec)
    //    {
    //        this(iterators, nowInSec, null);
    //    }

    //    private static DeletionTime collectPartitionLevelDeletion(List<AtomIterator> iterators)
    //    {
    //        DeletionTime delTime = DeletionTime.LIVE;
    //        for (AtomIterator iter : iterators)
    //            if (delTime.compareTo(iter.partitionLevelDeletion()) < 0)
    //                delTime = iter.partitionLevelDeletion();
    //        return delTime;
    //    }

    //    private static Row mergeStaticRows(List<AtomIterator> iterators, int nowInSec, MergeListener listener)
    //    {
    //        Columns staticColumns = iterators.get(0).staticColumns();
    //        if (staticColumns.isEmpty())
    //            return Rows.EMPTY_STATIC_ROW;

    //        Row[] toMerge = new Row[iterators.size()];
    //        for (int i = 0; i < iterators.size(); i++)
    //            toMerge[i] = iterators.get(i).staticRow();

    //        ReusableRow row = new ReusableRow(staticColumns, staticColumns.size());
    //        Rows.merge(EmptyClusteringPrefix.STATIC_PREFIX, toMerge, nowInSec, row.newWriter());
    //        return row;
    //    }

    //    public boolean isReverseOrder()
    //    {
    //        return isReverseOrder();
    //    }

    //    public Columns columns()
    //    {
    //        return columns;
    //    }

    //    public Columns staticColumns()
    //    {
    //        return staticColumns;
    //    }

    //    protected Atom computeNext()
    //    {
    //        while (mergeIterator.hasNext())
    //        {
    //            Atom atom = mergeIterator.next();
    //            if (atom != null)
    //                return atom;
    //        }
    //        return endOfData();
    //    }

    //    public CFMetaData metadata()
    //    {
    //        return metadata;
    //    }

    //    public Row staticRow()
    //    {
    //        return staticRow();
    //    }

    //    public DecoratedKey partitionKey()
    //    {
    //        return partitionKey;
    //    }

    //    public DeletionTime partitionLevelDeletion()
    //    {
    //        return partitionLevelDeletion;
    //    }

    //    public void close()
    //    {
    //        // This will close the input iterators
    //        FileUtils.closeQuietly(mergeIterator);
    //    }

    //    /**
    //     * Specific reducer for merge operations that rewrite the same reusable
    //     * row every time. This also skip cells shadowed by range tombstones when writing.
    //     */
    //    private class MergeReducer extends MergeIterator.Reducer<Atom, Atom> implements MergeListener
    //    {
    //        private final int size;

    //        private Atom.Kind nextKind;
    //        private ClusteringPrefix nextClustering;

    //        private final ReusableRow reader;
    //        private final ReusableRow.Writer writer;

    //        private final Rows.MergeHelper helper;
    //        private final Row[] rows;

    //        private ColumnDefinition prevColumn;
    //        private DeletionTime prevColumnDeletion;
    //        private DeletionTimeArray prevColumnDeletionVersions;

    //        private final DeletionTimeArray openMarkers;
    //        private final DeletionTimeArray.Cursor openMarkersCursor;
    //        private final RangeTombstoneMarker[] markers;

    //        private final ReusableRangeTombstoneMarker reusableMarker;

    //        private int openMarker = -1;

    //        private final MergeListener listener;

    //        private MergeReducer(int nowInSec, int size, Columns columns, MergeListener listener)
    //        {
    //            this.size = size;
    //            this.helper = new Rows.MergeHelper(nowInSec, size);

    //            // TODO: we could maybe do better for the estimation of the initial cell capacities of that container.
    //            // Myabe the AtomIterator interface could give use an estimate that we would average/max over all iterators?
    //            this.reader = new ReusableRow(columns, columns.size());
    //            this.writer = reader.newWriter();

    //            this.rows = new Row[size];
    //            this.prevColumnDeletionVersions = new DeletionTimeArray(size);

    //            this.openMarkers = new DeletionTimeArray(size);
    //            this.openMarkersCursor = openMarkers.newCursor();
    //            this.markers = new RangeTombstoneMarker[size];
    //            this.reusableMarker = new ReusableRangeTombstoneMarker();

    //            this.listener = listener;
    //        }

    //        public boolean trivialReduceIsTrivial()
    //        {
    //            return true;
    //        }

    //        public void reduce(int idx, Atom current)
    //        {
    //            nextKind = current.kind();
    //            nextClustering = current.clustering();
    //            switch (nextKind)
    //            {
    //                case ROW:
    //                    rows[idx] = (Row)current;
    //                    break;
    //                case RANGE_TOMBSTONE_MARKER:
    //                    markers[idx] = (RangeTombstoneMarker)current;
    //                    break;
    //            }
    //        }

    //        protected Atom getReduced()
    //        {
    //            switch (nextKind)
    //            {
    //                case ROW:
    //                    reader.reset();
    //                    Rows.merge(nextClustering, rows, helper, this);
    //                    for (int i = 0; i < size; i++)
    //                        rows[i] = null;

    //                    // Because shadowed cells are skipped, the row could be empty. In which case
    //                    // we return null and the enclosing iterator will just skip it.
    //                    return reader.isEmpty() ? null : reader;
    //                case RANGE_TOMBSTONE_MARKER:
    //                    int toReturn = -1;
    //                    boolean hasCloseMarker = false;
    //                    for (int i = 0; i < size; i++)
    //                    {
    //                        RangeTombstoneMarker marker = markers[i];
    //                        if (marker == null)
    //                            continue;

    //                        // We can completely ignore any marker that is shadowed by a partition level
    //                        // deletion
    //                        if (partitionLevelDeletion.supersedes(marker.delTime()))
    //                            continue;

    //                        // It's slightly easier to deal with close marker in a 2nd step
    //                        if (!marker.isOpenMarker())
    //                        {
    //                            hasCloseMarker = true;
    //                            continue;
    //                        }

    //                        DeletionTime dt = marker.delTime();
    //                        openMarkers.set(i, dt);
    //                        markers[i] = null;
    //                        // We only want to return that open marker is it's bigger than the current one
    //                        if (!openMarkers.supersedes(i, dt))
    //                            openMarker = toReturn = i;
    //                    }

    //                    if (hasCloseMarker)
    //                    {
    //                        for (int i = 0; i < size; i++)
    //                        {
    //                            RangeTombstoneMarker marker = markers[i];
    //                            if (marker == null)
    //                                continue;

    //                            // We've cleaned the open ones already
    //                            assert !marker.isOpenMarker();

    //                            openMarkers.clear(i);
    //                            // What we do depends on what the current open marker is. If it's not i, then we can just
    //                            // ignore that close. If it's i, then we need to find the next biggest open marker. If
    //                            // there is none, then we're closing the only open marker by returning this close marker,
    //                            // otherwise, that new biggest marker is the new open one and we should return it.
    //                            if (i == openMarker)
    //                            {
    //                                // We've cleaned i so updateOpenMarker will turn the second new biggest one
    //                                updateOpenMarker();
    //                                if (openMarker < 0)
    //                                {
    //                                    // We've closed the last open marker so not only should we return
    //                                    // this close marker, but we know we're done with the iteration here
    //                                    onMergedRangeTombstoneMarkers(nextClustering, false, marker.delTime(), markers);
    //                                    return reusableMarker.setTo(nextClustering, false, marker.delTime());
    //                                }

    //                                // Note that if toReturn is set at the beginning of this loop, it's necessarily equal
    //                                // to openMarker. So if we've closed the previous biggest open marker, it's ok to
    //                                // also update toReturn
    //                                toReturn = openMarker;
    //                            }
    //                        }
    //                    }

    //                    if (toReturn >= 0)
    //                    {
    //                        // Note that we can only arrive here if we have an open marker to return
    //                        openMarkersCursor.setTo(toReturn);
    //                        onMergedRangeTombstoneMarkers(nextClustering, true, openMarkersCursor, markers);
    //                        return reusableMarker.setTo(nextClustering, true, openMarkersCursor);
    //                    }
    //                    return null;
    //            }
    //            throw new AssertionError();
    //        }

    //        private void updateOpenMarker()
    //        {
    //            openMarker = -1;
    //            for (int i = 0; i < openMarkers.size(); i++)
    //            {
    //                if (openMarkers.isLive(i) && (openMarker < 0 || openMarkers.supersedes(i, openMarker)))
    //                    openMarker = i;
    //            }
    //        }

    //        protected void onKeyChange()
    //        {
    //            writer.reset();
    //            reader.reset();
    //        }

    //        public void onMergingRows(ClusteringPrefix clustering, long timestamp, Row[] versions)
    //        {
    //            // If the row is shadowed by a more recent deletion, delete it's timestamp
    //            long merged = partitionLevelDeletion.deletes(timestamp) || (openMarker >= 0 && openMarkersCursor.setTo(openMarker).deletes(timestamp))
    //                        ? Long.MIN_VALUE
    //                        : timestamp;

    //            if (listener != null)
    //                listener.onMergingRows(clustering, merged, versions);

    //            writer.setClustering(clustering);
    //            writer.setTimestamp(merged);
    //        }

    //        public void onMergedColumns(ColumnDefinition c, DeletionTime compositeDeletion, DeletionTimeArray versions)
    //        {
    //            // if the previous column had no cells but do have deletion info, write it now
    //            if (prevColumn != null && !prevColumnDeletion.isLive())
    //            {
    //                if (listener != null)
    //                    onMergedColumns(prevColumn, prevColumnDeletion, prevColumnDeletionVersions);
    //                writer.newColumn(prevColumn, prevColumnDeletion);
    //            }

    //            // We don't want to call writer.newColumn just yet because it could be that the whole column
    //            // ends up being fully shadowed by a top-level deletion or range tombstone
    //            prevColumn = c;
    //            prevColumnDeletion = partitionLevelDeletion.supersedes(compositeDeletion) || (openMarker >= 0 && openMarkersCursor.setTo(openMarker).supersedes(compositeDeletion))
    //                               ? DeletionTime.LIVE
    //                               : compositeDeletion.takeAlias();
    //            prevColumnDeletionVersions.copy(versions);
    //        }

    //        public void onMergedCells(Cell cell, Cell[] versions)
    //        {
    //            // Skip shadowed cells
    //            long timestamp = cell.timestamp();
    //            if (partitionLevelDeletion.deletes(timestamp)
    //                    || (openMarker > 0 && openMarkersCursor.setTo(openMarker).deletes(timestamp))
    //                    || prevColumnDeletion.deletes(timestamp))
    //            {
    //                if (listener != null)
    //                    onMergedCells(null, versions);
    //                return;
    //            }

    //            if (prevColumn != null)
    //            {
    //                if (listener != null)
    //                    onMergedColumns(prevColumn, prevColumnDeletion, prevColumnDeletionVersions);
    //                writer.newColumn(prevColumn, prevColumnDeletion);
    //                prevColumn = null;
    //            }

    //            if (listener != null)
    //                onMergedCells(cell, versions);
    //            writer.newCell(cell);
    //        }

    //        public void onRowDone()
    //        {
    //            if (prevColumn != null && !prevColumnDeletion.isLive())
    //            {
    //                if (listener != null)
    //                    onMergedColumns(prevColumn, prevColumnDeletion, prevColumnDeletionVersions);
    //                writer.newColumn(prevColumn, prevColumnDeletion);
    //            }

    //            prevColumn = null;
    //            writer.endOfRow();
    //        }

    //        public void onMergedRangeTombstoneMarkers(ClusteringPrefix clustering, boolean isOpenMarker, DeletionTime delTime, RangeTombstoneMarker[] versions)
    //        {
    //            if (listener != null)
    //                listener.onMergedRangeTombstoneMarkers(clustering, isOpenMarker, delTime, versions);
    //        }
    //    }
    //}
}
