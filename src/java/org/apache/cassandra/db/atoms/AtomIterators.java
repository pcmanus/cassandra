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
        public void onMergedComplexDeletion(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTime[] versions);
        public void onMergedCells(Cell mergedCell, Cell[] versions);
        public void onRowDone();

        public void onMergedRangeTombstoneMarkers(ClusteringPrefix prefix, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions);

        public void close();
    }

    /**
     * Returns whether the provided iterator has no data (including no deletion data).
     */
    public static boolean isEmpty(AtomIterator iterator)
    {
        return iterator.partitionLevelDeletion().isLive()
            && !iterator.hasNext()
            && iterator.staticRow().isEmpty();
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
        assert !iterators.isEmpty();
        if (iterators.size() == 1)
            return iterators.get(0);

        return AtomMergeIterator.create(iterators, nowInSec, null);
    }

    /**
     * Returns an iterator that is the result of merging other iterators, and using
     * specific MergeListener.
     *
     * Note that this method assumes that there is at least 2 iterators to merge.
     */
    public static AtomIterator merge(List<AtomIterator> iterators, int nowInSec, MergeListener mergeListener)
    {
        assert mergeListener != null;
        return AtomMergeIterator.create(iterators, nowInSec, mergeListener);
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

    // Please note that this is a destructive operation, only useful for debugging or if
    // you know what you'r doing!
    public static String toString(AtomIterator iterator)
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = iterator.metadata();
        PartitionColumns columns = iterator.columns();

        sb.append(String.format("[%s.%s] key=%s columns=%s reversed=%b deletion=%d\n",
                                metadata.ksName,
                                metadata.cfName,
                                metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                columns,
                                iterator.isReverseOrder(),
                                iterator.partitionLevelDeletion().markedForDeleteAt()));

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("-----\n").append(Rows.toString(metadata, iterator.staticRow()));

        while (iterator.hasNext())
        {
            Atom atom = iterator.next();
            if (atom.kind() == Atom.Kind.ROW)
                sb.append("\n-----\n").append(Rows.toString(metadata, (Row)atom));
            else
                sb.append("\n-----\n").append(RangeTombstoneMarkers.toString(metadata, (RangeTombstoneMarker)atom));
        }

        sb.append("\n-----\n");
        return sb.toString();
    }

    /**
     * A wrapper over MergeIterator to implement the AtomIterator interface.
     */
    private static class AtomMergeIterator extends AbstractAtomIterator
    {
        private final IMergeIterator<Atom, Atom> mergeIterator;
        private final MergeListener listener;

        private AtomMergeIterator(List<AtomIterator> iterators, PartitionColumns columns, DeletionTime partitionDeletion, int nowInSec, MergeListener listener)
        {
            super(iterators.get(0).metadata(),
                  iterators.get(0).partitionKey(),
                  partitionDeletion,
                  columns,
                  mergeStaticRows(iterators, columns.statics, nowInSec, listener, partitionDeletion),
                  iterators.get(0).isReverseOrder(),
                  mergeStats(iterators));

            this.listener = listener;
            this.mergeIterator = MergeIterator.get(iterators,
                                                   metadata.comparator.atomComparator,
                                                   new MergeReducer(iterators.size(), nowInSec));
        }

        private static AtomMergeIterator create(List<AtomIterator> iterators, int nowInSec, MergeListener listener)
        {
            assert inputIsValid(iterators);
            return new AtomMergeIterator(iterators, collectColumns(iterators), collectPartitionLevelDeletion(iterators), nowInSec, listener);
        }

        private static boolean inputIsValid(List<AtomIterator> iterators)
        {
            if (iterators.isEmpty())
                return false;

            AtomIterator first = iterators.get(0);
            for (int i = 1; i < iterators.size(); i++)
            {
                AtomIterator iter = iterators.get(i);
                if (!first.metadata().cfId.equals(iter.metadata().cfId))
                    return false;
                if (!first.partitionKey().equals(iter.partitionKey()))
                    return false;
                if (first.isReverseOrder() != iter.isReverseOrder())
                    return false;
            }
            return true;
        }

        private static DeletionTime collectPartitionLevelDeletion(List<AtomIterator> iterators)
        {
            DeletionTime delTime = DeletionTime.LIVE;
            for (AtomIterator iter : iterators)
                if (delTime.supersedes(iter.partitionLevelDeletion()))
                    delTime = iter.partitionLevelDeletion();
            return delTime;
        }

        private static Row mergeStaticRows(List<AtomIterator> iterators,
                                           Columns columns,
                                           int nowInSec,
                                           MergeListener listener,
                                           DeletionTime partitionDeletion)
        {
            if (columns.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            Rows.Merger merger = new Rows.Merger(iterators.size(), nowInSec, columns, listener);
            for (int i = 0; i < iterators.size(); i++)
                merger.add(i, iterators.get(i).staticRow());

            // Note that we should call 'takeAlias' on the result in theory, but we know that we
            // won't reuse the merger and so that it's ok not to.
            return merger.merge(partitionDeletion);
        }

        private static PartitionColumns collectColumns(List<AtomIterator> iterators)
        {
            PartitionColumns first = iterators.get(0).columns();
            Columns statics = first.statics;
            Columns regulars = first.regulars;
            for (int i = 1; i < iterators.size(); i++)
            {
                PartitionColumns cols = iterators.get(i).columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return statics == first.statics && regulars == first.regulars
                 ? first
                 : new PartitionColumns(statics, regulars);
        }

        private static AtomStats mergeStats(List<AtomIterator> iterators)
        {
            AtomStats stats = AtomStats.NO_STATS;
            for (AtomIterator iter : iterators)
                stats = stats.mergeWith(iter.stats());
            return stats;
        }

        protected Atom computeNext()
        {
            while (mergeIterator.hasNext())
            {
                Atom atom = mergeIterator.next();
                if (atom != null)
                    return atom;
            }
            return endOfData();
        }

        public void close()
        {
            // This will close the input iterators
            FileUtils.closeQuietly(mergeIterator);

            if (listener != null)
                listener.close();
        }

        /**
         * Specific reducer for merge operations that rewrite the same reusable
         * row every time. This also skip cells shadowed by range tombstones when writing.
         */
        private class MergeReducer extends MergeIterator.Reducer<Atom, Atom>
        {
            private Atom.Kind nextKind;

            private final Rows.Merger rowMerger;
            private final RangeTombstoneMarkers.Merger markerMerger;

            private MergeReducer(int size, int nowInSec)
            {
                this.rowMerger = new Rows.Merger(size, nowInSec, columns().regulars, listener);
                this.markerMerger = new RangeTombstoneMarkers.Merger(size, partitionLevelDeletion(), listener);
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return listener == null;
            }

            public void reduce(int idx, Atom current)
            {
                nextKind = current.kind();
                if (nextKind == Atom.Kind.ROW)
                    rowMerger.add(idx, (Row)current);
                else
                    markerMerger.add(idx, (RangeTombstoneMarker)current);
            }

            protected Atom getReduced()
            {
                return nextKind == Atom.Kind.ROW
                     ? rowMerger.merge(markerMerger.activeDeletion())
                     : markerMerger.merge();
            }

            protected void onKeyChange()
            {
                if (nextKind == Atom.Kind.ROW)
                    rowMerger.clear();
                else
                    markerMerger.clear();
            }
        }
    }
}
