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
import java.util.Comparator;
import java.util.Iterator;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Static methods to work on cells.
 */
public abstract class Cells
{
    public static final CounterContext counterContextManager = CounterContext.instance();

    private Cells() {}

    /**
     * Writes a tombstone cell to the provided writer.
     *
     * @param writer the {@code Row.Writer} to write the tombstone to.
     * @param column the column for the tombstone.
     * @param timestamp the timestamp for the tombstone.
     * @param localDeletionTime the local deletion time (in seconds) for the tombstone.
     */
    public static void writeTombstone(Row.Writer writer, ColumnDefinition column, long timestamp, int localDeletionTime)
    {
        writer.writeCell(column, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, SimpleLivenessInfo.forDeletion(timestamp, localDeletionTime), null);
    }

    /**
     * Computes the difference between a cell and the result of merging this
     * cell to other cells.
     * <p>
     * This method is used when cells from multiple sources are merged and we want to
     * find for a given source if it was up to date for that cell, and if not, what
     * should be sent to the source to repair it.
     *
     * @param merged the cell that is the result of merging multiple source.
     * @param cell the cell from one of the source that has been merged to yied
     * {@code merged}.
     * @return {@code null} if the source having {@code cell} is up-to-date for that
     * cell, or a cell that applied to the source will "repair" said source otherwise.
     */
    public static Cell diff(Cell merged, Cell cell)
    {
        // Note that it's enough to check if merged is a counterCell. If it isn't and
        // cell is one, it means that merged is a tombstone with a greater timestamp
        // than cell, because that's the only case where reconciling a counter with
        // a tombstone don't yield a counter. If that's the case, the normal path will
        // return what it should.
        if (merged.isCounterCell())
        {
            if (merged.livenessInfo().supersedes(cell.livenessInfo()))
                return merged;

            // Reconciliation never return something with a timestamp strictly lower than its operand. This
            // means we're in the case where merged.timestamp() == cell.timestamp(). As 1) tombstones win
            // over counters when timestamps are equal and 2) merged is a counter, it follows that cell
            // can't a tombstone or merge would be one too.
            assert !cell.isCounterCell();

            CounterContext.Relationship rel = counterContextManager.diff(merged.value(), cell.value());
            return (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT) ? merged : null;
        }
        return merged.livenessInfo().supersedes(cell.livenessInfo()) ? merged : null;
    }

    /**
     * Reconciles/merges two cells, one being an update to an existing cell,
     * yielding index updates if appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that those cells are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * Also note that which cell is provided as {@code existing} and which is
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param existing the pre-existing cell, the one that is updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cell, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy {@code existing} to
     * {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete both {@code existing} or {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the update is not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the timestamp delta between existing and update, or {@code Long.MAX_VALUE} if one
     * of them is {@code null} or deleted by {@code deletion}).
     */
    public static long reconcile(Clustering clustering,
                                 Cell existing,
                                 Cell update,
                                 DeletionTime deletion,
                                 Row.Writer writer,
                                 int nowInSec,
                                 SecondaryIndexManager.Updater indexUpdater)
    {
        if (existing == null || deletion.deletes(existing.livenessInfo()))
        {
            if (update == null || deletion.deletes(update.livenessInfo()))
                return Long.MAX_VALUE;

            indexUpdater.insert(clustering, update);
            update.writeTo(writer);
            return Long.MAX_VALUE;
        }

        if (update == null || deletion.deletes(update.livenessInfo()))
        {
            existing.writeTo(writer);
            return Long.MAX_VALUE;
        }

        Cell reconciled = reconcile(existing, update, nowInSec);
        reconciled.writeTo(writer);

        if (reconciled == update)
        {
            if (existing == null)
                indexUpdater.insert(clustering, reconciled);
            else
                indexUpdater.update(clustering, existing, reconciled);
        }
        return Math.abs(existing.livenessInfo().timestamp() - update.livenessInfo().timestamp());
    }

    /**
     * Reconciles/merge two cells.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that cell are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * This method is commutative over it's cells arguments: {@code reconcile(a, b, n) == reconcile(b, a, n)}.
     *
     * @param c1 the first cell participating in the reconciliation.
     * @param c2 the second cell participating in the reconciliation.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     *
     * @return a cell corresponding to the reconciliation of {@code c1} and {@code c2}.
     * For non-counter cells, this will always be either {@code c1} or {@code c2}, but for
     * counter cells this can be a newly allocated cell.
     */
    public static Cell reconcile(Cell c1, Cell c2, int nowInSec)
    {
        if (c1 == null)
            return c2 == null ? null : c2;
        if (c2 == null)
            return c1;

        if (c1.isCounterCell() || c2.isCounterCell())
        {
            boolean c1Live = c1.isLive(nowInSec);
            boolean c2Live = c2.isLive(nowInSec);
            // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
            if (!c1Live)
                // c1 is a tombstone: it has precedence over c2 if either c2 is not a tombstone, or c1 has a greater timestamp
                return c2Live || c1.livenessInfo().supersedes(c2.livenessInfo()) ? c1 : c2;

            // If c2 is a tombstone, since c1 isn't one, it has precedence
            if (!c2Live)
                return c2;

            // live + live. return one of the cells if its context is a superset of the other's, or merge them otherwise
            ByteBuffer value = Cells.counterContextManager.merge(c1.value(), c2.value());

            LivenessInfo mergedInfo = c1.livenessInfo().mergeWith(c2.livenessInfo());
            if (value == c1.value() && mergedInfo == c1.livenessInfo())
                return c1;
            else if (value == c2.value() && mergedInfo == c2.livenessInfo())
                return c2;
            else // merge clocks and timestamps.
                return new SimpleCounter(c1.column(), value, mergedInfo);
        }

        long ts1 = c1.livenessInfo().timestamp(), ts2 = c2.livenessInfo().timestamp();
        if (ts1 != ts2)
            return ts1 < ts2 ? c2 : c1;
        boolean c1Live = c1.isLive(nowInSec);
        if (c1Live != c2.isLive(nowInSec))
            return c1Live ? c2 : c1;

        int c = c1.value().compareTo(c2.value());
        if (c < 0)
            return c2;
        else if (c > 0)
            return c1;

        // Prefer the longest ttl if relevant
        return c1.livenessInfo().localDeletionTime() < c2.livenessInfo().localDeletionTime() ? c2 : c1;
    }

    /**
     * Computes the reconciliation of a complex column given its pre-existing
     * cells and the ones it is updated with, and generating index update if
     * appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that the cells are for the same row and same
     * complex column.
     * <p>
     * Also note that which cells is provided as {@code existing} and which are
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param column the complex column the cells are for.
     * @param existing the pre-existing cells, the ones that are updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cells, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy the cells from
     * {@code existing} to {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete cells in both {@code existing} and {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the updates are not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the smallest timestamp delta between corresponding cells from existing and update. A
     * timestamp delta being computed as the difference between a cell from {@code update} and the
     * cell in {@code existing} having the same cell path (if such cell exists). If the intersection
     * of cells from {@code existing} and {@code update} having the same cell path is empty, this
     * returns {@code Long.MAX_VALUE}.
     */
    public static long reconcileComplex(Clustering clustering,
                                        ColumnDefinition column,
                                        Iterator<Cell> existing,
                                        Iterator<Cell> update,
                                        DeletionTime deletion,
                                        Row.Writer writer,
                                        int nowInSec,
                                        SecondaryIndexManager.Updater indexUpdater)
    {
        Comparator<CellPath> comparator = column.cellPathComparator();
        Cell nextExisting = getNext(existing);
        Cell nextUpdate = getNext(update);
        long timeDelta = Long.MAX_VALUE;
        while (nextExisting != null || nextUpdate != null)
        {
            int cmp = nextExisting == null ? 1
                     : (nextUpdate == null ? -1
                     : comparator.compare(nextExisting.path(), nextUpdate.path()));
            if (cmp < 0)
            {
                reconcile(clustering, nextExisting, null, deletion, writer, nowInSec, indexUpdater);
                nextExisting = getNext(existing);
            }
            else if (cmp > 0)
            {
                reconcile(clustering, null, nextUpdate, deletion, writer, nowInSec, indexUpdater);
                nextUpdate = getNext(update);
            }
            else
            {
                timeDelta = Math.min(timeDelta, reconcile(clustering, nextExisting, nextUpdate, deletion, writer, nowInSec, indexUpdater));
                nextExisting = getNext(existing);
                nextUpdate = getNext(update);
            }
        }
        return timeDelta;
    }

    private static Cell getNext(Iterator<Cell> iterator)
    {
        return iterator == null || !iterator.hasNext() ? null : iterator.next();
    }

    /**
     * A simple implementation of a counter cell used when reconciling two counter cell
     * requires to allocate a new cell object (that is when the value of the reconciled
     * counter cells needs to be merged).
     */
    private static class SimpleCounter extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        private SimpleCounter(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return true;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }
}
