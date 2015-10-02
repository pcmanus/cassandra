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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
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
        if (helper.version >= MessagingService.VERSION_30)
            return new CurrentDeserializer(metadata, in, header, helper);
        else
            return new OldFormatDeserializer(metadata, in, helper, partitionDeletion, readAllAsDynamic);
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
    public abstract int compareNextTo(Slice.Bound bound) throws IOException;

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

        public int compareNextTo(Slice.Bound bound) throws IOException
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
                RangeTombstone.Bound bound = clusteringDeserializer.deserializeNextBound();
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

    public static class OldFormatDeserializer extends UnfilteredDeserializer
    {
        private final boolean readAllAsDynamic;
        private boolean skipStatic;

        private boolean inputExhausted;

        // The next Unfiltered to return, computed by hasNext()
        private Unfiltered next;

        // In some condition, we don't use the last atom read from the input for the current iteration
        // (typically, because we need to close an open tombstone range first). In that case we use
        // nextAtom to save the read but still unused atom. It is then pick up by the next iteration before
        // reaching into the input.
        private LegacyLayout.LegacyAtom nextAtom;

        private final LegacyLayout.CellGrouper grouper;
        private final TombstoneTracker tombstoneTracker;

        private OldFormatDeserializer(CFMetaData metadata,
                                      DataInputPlus in,
                                      SerializationHelper helper,
                                      DeletionTime partitionDeletion,
                                      boolean readAllAsDynamic)
        {
            super(metadata, in, helper);
            this.readAllAsDynamic = readAllAsDynamic;
            this.grouper = new LegacyLayout.CellGrouper(metadata, helper);
            this.tombstoneTracker = new TombstoneTracker(partitionDeletion);
        }

        public void setSkipStatic()
        {
            this.skipStatic = true;
        }

        public boolean hasNext() throws IOException
        {
            LegacyLayout.LegacyAtom atom = null;

            while (next == null)
            {
                // Deserialize the next Unfiltered to return, updating the 'next' field.
                if (atom == null)
                {
                    if (nextAtom == null)
                    {
                        atom = readNextAtom();
                        if (atom == null)
                            return false;
                    }
                    else
                    {
                        atom = nextAtom;
                        nextAtom = null;
                    }
                }

                // If a range tombstone closes strictly before this atom, we need to return that close (or boundary) marker first.
                if (tombstoneTracker.hasClosingMarkerBefore(atom))
                {
                    nextAtom = atom;
                    next = tombstoneTracker.popClosingMarker();
                    return true;
                }

                if (isRow(atom))
                {
                    // Read the full row
                    LegacyLayout.CellGrouper grouper = atom.isStatic()
                                                     ? LegacyLayout.CellGrouper.staticGrouper(metadata, helper)
                                                     : this.grouper;

                    grouper.reset();
                    grouper.addAtom(atom);
                    while ((atom = readNextAtom()) != null)
                    {
                        // Add the atom to the grouper. If it's not an atom belonging to the built row, we're done for this
                        // row and should the current atom for the next iteration
                        if (!grouper.addAtom(atom))
                        {
                            assert nextAtom == null;
                            nextAtom = atom;
                            break;
                        }
                    }
                    atom = null;
                    Row row = grouper.getRow();

                    // When reading old tables, we sometimes want to skip static data (due to how staticly defined column of compact tables are
                    // handled). So if we're asked to, ignore if it's static.
                    if (!(skipStatic && row.isStatic()))
                        next = row;
                }
                else
                {
                    LegacyLayout.LegacyRangeTombstone tombstone = atom.asRangeTombstone();
                    atom = null; // It's possible this tombstone doesn't generate something to return right away. That's the case if the
                                 // beginning of the new range is shadowed by the currently open range for instance (but the end is not
                                 // shadowed so that the atom wasn't skipped by readNextAtom). In that case, we'll want to clear atom for
                                 // the next iteration, so clear it now (we'll use the tombstone variable in what follow anyway).

                    // The sstable iterators assume that if there is one, the static row is the first thing this deserializer will return.
                    // However, in the old format, a range tombstone with an empty start would sort before any static cell. So we should
                    // detect that case and invert the return order if necessary.
                    if (tombstone.start.bound.size() == 0)
                    {
                        LegacyLayout.LegacyAtom followingAtom = readNextAtom();
                        if (followingAtom != null && followingAtom.isStatic())
                        {
                            // We have a both a RT starting from the beginning of the partition, and a static row. So set
                            // atom to the static row so it's returned next, and save the RT for after that.
                            atom = followingAtom;
                            nextAtom = tombstone;
                            continue;
                        }
                        else
                        {
                            // The following atom isn't static, so return the RT normally but save that following atom for after
                            nextAtom = followingAtom;
                        }
                    }

                    // We have an opening range. What we do depends on what already opened range we have. 
                    next = tombstoneTracker.openNew(tombstone);
                }
            }
            // We get here is next != null so we have something to return
            return true;
        }

        // Returns null if we're done
        private LegacyLayout.LegacyAtom readNextAtom() throws IOException
        {
            while (!inputExhausted)
            {
                LegacyLayout.LegacyAtom atom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                // We don't want to return shadowed data because that would fail the contract
                // of UnfilteredRowIterator. However the old format could have shadowed data, so filter it here.
                if (atom == null)
                    inputExhausted = true;
                else if (!tombstoneTracker.isShadowed(atom))
                    return atom;
            }
            return null;
        }

        private boolean isRow(LegacyLayout.LegacyAtom atom)
        {
            if (atom.isCell())
                return true;

            LegacyLayout.LegacyRangeTombstone tombstone = atom.asRangeTombstone();
            return tombstone.isCollectionTombstone() || tombstone.isRowDeletion(metadata);
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            assert hasNext();
            return metadata.comparator.compare(next.clustering(), bound);
        }

        public boolean nextIsRow() throws IOException
        {
            assert hasNext();
            return next.isRow();
        }

        public boolean nextIsStatic() throws IOException
        {
            return nextIsRow() && ((Row)next).isStatic();
        }

        public Unfiltered readNext() throws IOException
        {
            assert hasNext();
            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }

        public void skipNext() throws IOException
        {
            assert hasNext();
            next = null;
        }

        public void clearState()
        {
            next = null;
            nextAtom = null;
            inputExhausted = false;
        }

        /**
         * Tracks which range tombstones are open when deserializing the old format.
         */
        private class TombstoneTracker
        {
            private final DeletionTime partitionDeletion;

            // Open tombstones sorted by their closing bound (i.e. first tombstone is the first to close).
            // As we only track non-fully-shadowed ranges, the first range is necessarily the currently
            // open tombstone (the one with the higher timestamp).
            private final SortedSet<LegacyLayout.LegacyRangeTombstone> openTombstones;

            public TombstoneTracker(DeletionTime partitionDeletion)
            {
                this.partitionDeletion = partitionDeletion;
                this.openTombstones = new TreeSet<>((rt1, rt2) -> metadata.comparator.compare(rt1.stop.bound, rt2.stop.bound));
            }

            /**
             * Checks if the provided atom is fully shadowed by the open tombstones of this tracker (or the partition deletion).
             */
            public boolean isShadowed(LegacyLayout.LegacyAtom atom)
            {
                long timestamp = atom.isCell() ? atom.asCell().timestamp : atom.asRangeTombstone().deletionTime.markedForDeleteAt();

                if (partitionDeletion.deletes(timestamp))
                    return true;

                for (LegacyLayout.LegacyRangeTombstone tombstone : openTombstones)
                {
                    if (tombstone.deletionTime.deletes(timestamp))
                    {
                        // If it's a cell, then it's shadowed. If it's a RT, it's only shadowed if it ends before the tombstone we test against
                        return atom.isCell() || metadata.comparator.compare(atom.asRangeTombstone().stop.bound, tombstone.stop.bound) <= 0;
                    }
                }

                return false;
            }

            /**
             * Whether the currently open marker closes stricly before the provided atom.
             */
            public boolean hasClosingMarkerBefore(LegacyLayout.LegacyAtom atom)
            {
                return !openTombstones.isEmpty()
                    && metadata.comparator.compare(openTombstones.first().stop.bound, atom.clustering()) < 0;
            }

            /**
             * Returns the unfiltered corresponding to closing the currently open marker (and update the tracker accordingly).
             */
            public Unfiltered popClosingMarker()
            {
                assert !openTombstones.isEmpty();

                Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                LegacyLayout.LegacyRangeTombstone first = iter.next();
                iter.remove();

                // If that was the last open tombstone, we just want to close it. Otherwise, we have a boundary with the
                // next tombstone
                if (!iter.hasNext())
                    return new RangeTombstoneBoundMarker(first.stop.bound, first.deletionTime);

                LegacyLayout.LegacyRangeTombstone next = iter.next();
                return RangeTombstoneBoundaryMarker.makeBoundary(false, first.stop.bound, first.stop.bound.invert(), first.deletionTime, next.deletionTime);
            }

            /**
             * Update the tracker given the provided newly open tombstone. This return the Unfiltered corresponding to the opening
             * of said tombstone: this can be a simple open mark, a boundary (if there was an open tombstone superseded by this new one)
             * or even null (if the new tombston start is supersedes by the currently open tombstone).
             *
             * Note that this method assume the added tombstone is not fully shadowed, i.e. that !isShadowed(tombstone). It also
             * assumes no opened tombstone closes before that tombstone (so !hasClosingMarkerBefore(tombstone)).
             */
            public Unfiltered openNew(LegacyLayout.LegacyRangeTombstone tombstone)
            {
                if (openTombstones.isEmpty())
                {
                    openTombstones.add(tombstone);
                    return new RangeTombstoneBoundMarker(tombstone.start.bound, tombstone.deletionTime);
                }

                Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                LegacyLayout.LegacyRangeTombstone first = iter.next();
                if (tombstone.deletionTime.supersedes(first.deletionTime))
                {
                    // We're supperseding the currently open tombstone, so we should produce a boundary that close the currently open
                    // one and open the new one. We should also add the tombstone, but if it stop after the first one, we should
                    // also remove that first tombstone as it won't be useful anymore.
                    if (metadata.comparator.compare(tombstone.stop.bound, first.stop.bound) >= 0)
                        iter.remove();

                    openTombstones.add(tombstone);
                    return RangeTombstoneBoundaryMarker.makeBoundary(false, tombstone.start.bound.invert(), tombstone.start.bound, first.deletionTime, tombstone.deletionTime);
                }
                else
                {
                    // If the new tombstone don't supersedes the currently open tombstone, we don't have anything to return, we
                    // just add the new tombstone (because we know tombstone is not fully shadowed, this imply the new tombstone
                    // simply extend after the first one and we'll deal with it later)
                    assert metadata.comparator.compare(tombstone.start.bound, first.stop.bound) > 0;
                    openTombstones.add(tombstone);
                    return null;
                }
            }

            public boolean hasOpenTombstones()
            {
                return !openTombstones.isEmpty();
            }

            private boolean formBoundary(LegacyLayout.LegacyRangeTombstone close, LegacyLayout.LegacyRangeTombstone open)
            {
                return metadata.comparator.compare(close.stop.bound, open.start.bound) == 0;
            }
        }
    }
}
