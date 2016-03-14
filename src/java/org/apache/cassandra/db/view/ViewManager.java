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
package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;


/**
 * Manages {@link View}'s for a single {@link ColumnFamilyStore}. All of the views for that table are created when this
 * manager is initialized.
 *
 * The main purposes of the manager are to provide a single location for updates to be vetted to see whether they update
 * any views {@link ViewManager#updatesAffectView(Collection, boolean)}, provide locks to prevent multiple
 * updates from creating incoherent updates in the view {@link ViewManager#acquireLockFor(ByteBuffer)}, and
 * to affect change on the view.
 *
 * TODO: it might make more sense to promote TableViews to its own class and move most of the logic of ViewManager there
 * since anything related to an update is intrinsically bound to a single table.
 */
public class ViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(ViewManager.class);

    /**
     * Groups all the views for a given table.
     */
    public class TableViews extends AbstractCollection<View>
    {
        // We need this to be thread-safe, but the number of times this is changed (when a view is created in the keyspace)
        // massively exceeds the number of time it's read (for every mutation on the keyspace), so a copy-on-write list is the best option.
        private final List<View> views = new CopyOnWriteArrayList();

        private TableViews()
        {
        }

        public int size()
        {
            return views.size();
        }

        public Iterator<View> iterator()
        {
            return views.iterator();
        }

        public boolean add(View view)
        {
            return views.add(view);
        }

        public Iterable<ColumnFamilyStore> allViewsCfs()
        {
            return Iterables.transform(views, view -> keyspace.getColumnFamilyStore(view.getDefinition().viewName));
        }

        public void forceBlockingFlush()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.forceBlockingFlush();
        }

        public void dumpMemtables()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.dumpMemtable();
        }

        public void truncateBlocking(long truncatedAt)
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
            {
                ReplayPosition replayAfter = viewCfs.discardSSTables(truncatedAt);
                SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter);
            }
        }

        private void removeByName(String viewName)
        {
        }
    }

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentViewWriters() * 1024);

    private static final boolean enableCoordinatorBatchlog = Boolean.getBoolean("cassandra.mv_enable_coordinator_batchlog");

    private final ConcurrentMap<String, View> viewsByName = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, TableViews> viewsByBaseTable = new ConcurrentHashMap<>();
    private final Keyspace keyspace;

    public ViewManager(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    /**
     * Calculates and pushes updates to the views replicas. The replicas are determined by
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     */
    public void pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog, AtomicLong baseComplete)
    {
        Collection<View> views = updatedViews(update);
        if (views.isEmpty())
            return;

        // Read modified rows
        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = readExistingRowsCommand(update, views, nowInSec);
        if (command == null)
            return;

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(update.metadata());
        long start = System.nanoTime();
        Collection<Mutation> mutations;
        try (ReadOrderGroup orderGroup = command.startOrderGroup();
             UnfilteredRowIterator existings = UnfilteredPartitionIterators.getOnlyElement(command.executeLocally(orderGroup), command);
             UnfilteredRowIterator updates = update.unfilteredIterator())
        {
            mutations = generateViewUpdates(views, updates, existings, nowInSec);
        }
        Keyspace.openAndGetStore(update.metadata()).metric.viewReadTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (!mutations.isEmpty())
            StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog, baseComplete);
    }

    /**
     * Return the views that are potentially updated by the provided updates.
     *
     * @param updates the updates applied to the base table.
     * @return the views affected by {@code updates}.
     */
    private Collection<View> updatedViews(PartitionUpdate updates)
    {
        Collection<View> tableViews = viewsByBaseTable.get(updates.metadata().cfId);
        List<View> matchingViews = new ArrayList<>(tableViews.size());

        for (View view : tableViews)
        {
            ReadQuery selectQuery = view.getReadQuery();
            if (!selectQuery.selectsKey(updates.partitionKey()))
                continue;

            matchingViews.add(view);
        }
        return matchingViews;
    }

    /**
     * Returns the command to use to read the existing rows required to generate view updates for the provided base
     * base updates.
     *
     * @param updates the base table updates being applied.
     * @param views the views potentially affected by {@code updates}.
     * @param nowInSec the current time in seconds.
     * @return the command to use to read the base table rows required to generate view updates for {@code updates}.
     */
    private SinglePartitionReadCommand readExistingRowsCommand(PartitionUpdate updates, Collection<View> views, int nowInSec)
    {
        Slices.Builder sliceBuilder = null;
        DeletionInfo deletionInfo = updates.deletionInfo();
        CFMetaData metadata = updates.metadata();
        DecoratedKey key = updates.partitionKey();
        // TODO: This is subtle: we need to gather all the slices that we have to fetch between partition del, range tombstones and rows.
        if (!deletionInfo.isLive())
        {
            sliceBuilder = new Slices.Builder(metadata.comparator);
            // Everything covered by a deletion might invalidate an existing view entry, which means we must read it to know. In practice
            // though, the views involved might filter some base table clustering columns, in which case we can restrict what we read
            // using those restrictions.
            // If there is a partition deletion, then we can simply take each slices from each view select filter. They may overlap but
            // the Slices.Builder handles that for us. Note that in many case this will just involve reading everything (as soon as any
            // view involved has no clustering restrictions for instance).
            // For range tombstone, we should theoretically take the difference between the range tombstoned and the slices selected
            // by every views, but as we don't an easy way to compute that right now, we keep it simple and just use the tombstoned
            // range.
            // TODO: we should improve that latter part.
            if (!deletionInfo.getPartitionDeletion().isLive())
            {
                for (View view : views)
                    sliceBuilder.addAll(view.getSelectStatement().clusteringIndexFilterAsSlices());
            }
            else
            {
                assert deletionInfo.hasRanges();
                Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
                while (iter.hasNext())
                    sliceBuilder.add(iter.next().deletedSlice());
            }
        }

        // We need to read every row that is updated, unless we can prove that it has no impact on any view entries.

        // If we had some slices from the deletions above, we'll continue using that. Otherwise, it's more efficient to build
        // a names query.
        BTreeSet.Builder<Clustering> namesBuilder = sliceBuilder == null ? BTreeSet.builder(metadata.comparator) : null;
        for (Row row : updates)
        {
            // Don't read the existing state if we can prove the update won't affect any views
            if (!affectsAnyViews(key, row, views))
                continue;

            if (namesBuilder == null)
                sliceBuilder.add(Slice.make(row.clustering()));
            else
                namesBuilder.add(row.clustering());
        }

        NavigableSet<Clustering> names = namesBuilder == null ? null : namesBuilder.build();
        // If we have a slice builder, it means we had some deletions and we have to read. But if we had
        // only row updates, it's possible none of them affected the views, in which case we have nothing
        // to do.
        if (names != null && names.isEmpty())
            return null;

        ClusteringIndexFilter clusteringFilter = names == null
                                               ? new ClusteringIndexSliceFilter(sliceBuilder.build(), false)
                                               : new ClusteringIndexNamesFilter(names, false);
        // If we have more than one view, we should merge the queried columns by each views but to keep it simple we just
        // include everything. We could change that in the future.
        ColumnFilter queriedColumns = views.size() == 1
                                    ? Iterables.getOnlyElement(views).getSelectStatement().queriedColumns()
                                    : ColumnFilter.all(metadata);
        // Note that the views could have restrictions on regular columns, but even if that's the case we shouldn't apply those
        // when we read, because even if an existing row doesn't match the view filter, the update can change that in which
        // case we'll need to know the existing content. There is also no easy way to merge those RowFilter when we have multiple views.
        // TODO: we could still make sense to special case for when there is a single view and a small number of updates (and
        // no deletions). Indeed, in that case we could check whether any of the update modify any of the restricted regular
        // column, and if that's not the case we could use view filter. We keep it simple for now though.
        RowFilter rowFilter = RowFilter.NONE;
        return SinglePartitionReadCommand.create(metadata, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, clusteringFilter);
    }

    private boolean affectsAnyViews(DecoratedKey partitionKey, Row update, Collection<View> views)
    {
        for (View view : views)
        {
            if (view.mayBeAffectedBy(partitionKey, update))
                return true;
        }
        return false;
    }

    /**
     * Given some base table updates and the existing values for the rows affected by that update, generates the mutation to
     * be applied to the provided views.
     *
     * @param views the views potentially affected by {@code updates}.
     * @param updates the base table updates being applied.
     * @param existings the existing values for the rows affected by {@code updates}. This is used to decide if a view is
     * obsoleted by the update and should be removed, gather the values for columns that may not be part of the update if
     * a new view entry needs to be created, and compute the minimal updates to be applied if the view entry isn't changed
     * but has simply some updated values. This will be empty for view building as we want to assume anything we'll pass
     * to {@code updates} is new.
     * @param nowInSec the current time in seconds.
     * @return the mutations to apply to the {@code views}. This can be empty.
     */
    public Collection<Mutation> generateViewUpdates(Collection<View> views, UnfilteredRowIterator updates, UnfilteredRowIterator existings, int nowInSec)
    {
        MultiViewUpdateBuilder builder = new MultiViewUpdateBuilder(updates.metadata(), views, updates.partitionKey(), nowInSec);
        ClusteringComparator comparator = updates.metadata().comparator;

        DeletionTracker existingsDeletion = new DeletionTracker(existings.partitionLevelDeletion());
        DeletionTracker updatesDeletion = new DeletionTracker(updates.partitionLevelDeletion());

        /*
         * We iterate through the updates and the existing rows in parallel. This allows us to know the consequence
         * on the view of each update.
         */
        PeekingIterator<Unfiltered> existingsIter = Iterators.peekingIterator(existings);
        PeekingIterator<Unfiltered> updatesIter = Iterators.peekingIterator(updates);

        while (existingsIter.hasNext() && updatesIter.hasNext())
        {
            Unfiltered existing = existingsIter.peek();
            Unfiltered update = updatesIter.peek();

            Row existingRow;
            Row updateRow;
            int cmp = comparator.compare(update, existing);
            if (cmp < 0)
            {
                // We have an update where there was nothing before
                if (update.isRangeTombstoneMarker())
                {
                    updatesDeletion.update(updatesIter.next());
                    continue;
                }

                updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
                existingRow = emptyRow(updateRow.clustering(), existingsDeletion.currentDeletion());
            }
            else if (cmp > 0)
            {
                // We have something existing but no update (which will happen either because it's a range tombstone marker in
                // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
                if (existing.isRangeTombstoneMarker())
                {
                    existingsDeletion.update(existingsIter.next());
                    continue;
                }

                existingRow = ((Row)existingsIter.next()).withRowDeletion(existingsDeletion.currentDeletion());
                updateRow = emptyRow(existingRow.clustering(), updatesDeletion.currentDeletion());

                // The way we build the read command used for existing rows, we should always have updatesDeletion.currentDeletion()
                // that is not live, since we wouldn't have read the existing row otherwise. And we could assert that, but if we ever
                // change the read method so that it can slightly over-read in some case, that would be an easily avoiding bug lurking,
                // so we just handle the case.
                if (updateRow == null)
                    continue;
            }
            else
            {
                // We're updating a row that had pre-existing data
                if (update.isRangeTombstoneMarker())
                {
                    assert existing.isRangeTombstoneMarker();
                    updatesDeletion.update(updatesIter.next());
                    existingsDeletion.update(existingsIter.next());
                    continue;
                }

                assert !existing.isRangeTombstoneMarker();
                existingRow = ((Row)existingsIter.next()).withRowDeletion(existingsDeletion.currentDeletion());
                updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
            }

            builder.generateViewsMutations(existingRow, updateRow);
        }

        // We only care about more existing rows if the update deletion isn't live, i.e. if we had a partition deletion
        if (!updatesDeletion.currentDeletion().isLive())
        {
            while (existingsIter.hasNext())
            {
                Unfiltered existing = existingsIter.next();
                // If it's a range tombstone, we don't care, we're only looking for existing entry that gets deleted by
                // the new partition deletion
                if (existing.isRangeTombstoneMarker())
                    continue;

                Row existingRow = (Row)existing;
                builder.generateViewsMutations(existingRow, emptyRow(existingRow.clustering(), updatesDeletion.currentDeletion()));
            }
        }
        while (updatesIter.hasNext())
        {
            Unfiltered update = updatesIter.next();
            // If it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it for view updates
            if (update.isRangeTombstoneMarker())
                continue;

            Row updateRow = (Row)update;
            builder.generateViewsMutations(emptyRow(updateRow.clustering(), DeletionTime.LIVE), updateRow);
        }
        return builder.build();
    }

    private static Row emptyRow(Clustering clustering, DeletionTime deletion)
    {
        // Returning null for an empty row is slightly ugly, but the case where there is no pre-existing row is fairly common
        // (especially when building the view), so we want to avoid a dummy allocation of an empty row every time.
        // And MultiViewUpdateBuilder knows how to deal with that.
        return deletion.isLive() ? null : BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(deletion));
    }

    /**
     * A simple helper that tracks for a given {@code UnfilteredRowIterator} what is the current deletion at any time of the
     * iteration. It will be the currently open range tombstone deletion if there is one and the partition deletion otherwise.
     */
    private class DeletionTracker
    {
        private final DeletionTime partitionDeletion;
        private DeletionTime deletion;

        public DeletionTracker(DeletionTime partitionDeletion)
        {
            this.partitionDeletion = partitionDeletion;
        }

        public void update(Unfiltered marker)
        {
            assert marker instanceof RangeTombstoneMarker;
            RangeTombstoneMarker rtm = (RangeTombstoneMarker)marker;
            this.deletion = rtm.isOpen(false)
                          ? rtm.openDeletionTime(false)
                          : null;
        }

        public DeletionTime currentDeletion()
        {
            return deletion == null ? partitionDeletion : deletion;
        }
    }

    public boolean updatesAffectView(Collection<? extends IMutation> mutations, boolean coordinatorBatchlog)
    {
        if (coordinatorBatchlog && !enableCoordinatorBatchlog)
            return false;

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                assert keyspace.getName().equals(update.metadata().ksName);

                if (coordinatorBatchlog && keyspace.getReplicationStrategy().getReplicationFactor() == 1)
                    continue;

                if (!updatedViews(update).isEmpty())
                    return true;
            }
        }

        return false;
    }

    public Iterable<View> allViews()
    {
        return viewsByName.values();
    }

    public void update(String viewName)
    {
        View view = viewsByName.get(viewName);
        assert view != null : "When updating a view, it should already be in the ViewManager";
        view.build();

        // We provide the new definition from the base metadata
        Optional<ViewDefinition> viewDefinition = keyspace.getMetadata().views.get(viewName);
        assert viewDefinition.isPresent() : "When updating a view, it should still be in the Keyspaces views";
        view.updateDefinition(viewDefinition.get());
    }

    public void reload()
    {
        Map<String, ViewDefinition> newViewsByName = new HashMap<>();
        for (ViewDefinition definition : keyspace.getMetadata().views)
        {
            newViewsByName.put(definition.viewName, definition);
        }

        for (String viewName : viewsByName.keySet())
        {
            if (!newViewsByName.containsKey(viewName))
                removeView(viewName);
        }

        for (Map.Entry<String, ViewDefinition> entry : newViewsByName.entrySet())
        {
            if (!viewsByName.containsKey(entry.getKey()))
                addView(entry.getValue());
        }

        for (View view : allViews())
        {
            view.build();
            // We provide the new definition from the base metadata
            view.updateDefinition(newViewsByName.get(view.name));
        }
    }

    public void addView(ViewDefinition definition)
    {
        View view = new View(definition, keyspace.getColumnFamilyStore(definition.baseTableId));
        forTable(view.getDefinition().baseTableId).add(view);
        viewsByName.put(definition.viewName, view);
    }

    public void removeView(String name)
    {
        View view = viewsByName.remove(name);

        if (view == null)
            return;

        forTable(view.getDefinition().baseTableId).removeByName(name);
        SystemKeyspace.setViewRemoved(keyspace.getName(), view.name);
    }

    public void buildAllViews()
    {
        for (View view : allViews())
            view.build();
    }

    public TableViews forTable(UUID baseId)
    {
        TableViews views = viewsByBaseTable.get(baseId);
        if (views == null)
        {
            views = new TableViews();
            TableViews previous = viewsByBaseTable.putIfAbsent(baseId, views);
            if (previous != null)
                views = previous;
        }
        return views;
    }

    public static Lock acquireLockFor(ByteBuffer key)
    {
        Lock lock = LOCKS.get(key);

        if (lock.tryLock())
            return lock;

        return null;
    }
}
