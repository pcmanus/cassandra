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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Represents which (non-PK) columns (and optionally which sub-part of a column for complex columns) are selected
 * by a query.
 *
 * We distinguish 2 sets of columns in practice: the _fetched_ columns, which are the columns that we (may, see
 * below) need to fetch internally, and the _queried_ columns, which are the columns that the user has selected
 * in its request.
 *
 * The reason for distinguishing those 2 sets is that due to the CQL semantic (see #6588 for more details), we
 * often need to internally fetch all regular columns for the queried table, but can still do some optimizations for
 * those columns that are not directly queried by the user (see #10657 for more details).
 *
 * Note that in practice:
 *   - the _queried_ columns set is always included in the _fetched_ one.
 *   - whenever those sets are different, we know 1) the _fetched_ set contains all regular columns for the table and 2)
 *     _fetched_ == _queried_ for static columns, so we don't have to record this set, we just keep a pointer to the
 *     table metadata. The only set we concretely store is thus the _queried_ one.
 *   - in the special case of a {@code SELECT *} query, we want to query all columns, and _fetched_ == _queried.
 *     As this is a common case, we special case it by keeping the _queried_ set {@code null} (and we retrieve
 *     the columns through the metadata pointer).
 *
 * For complex columns, this class optionally allows to specify a subset of the cells to query for each column.
 * We can either select individual cells by path name, or a slice of them. Note that this is a sub-selection of
 * _queried_ cells, so if _fetched_ != _queried_, then the cell selected by this sub-selection are considered
 * queried and the other ones are considered fetched (and if a column has some sub-selection, it must be a queried
 * column, which is actually enforced by the Builder below).
 */
public class ColumnFilter
{
    public static final Serializer serializer = new Serializer();

    // True if _fetched_ includes all regular columns (an any static in _queried_), in which case metadata must not be
    // null. If false, then _fetched_ == _queried_ and we only store _queried_.
    private final boolean isFetchAllRegulars;

    private final CFMetaData metadata; // can be null if !isFetchAllRegulars

    private final PartitionColumns queried; // can be null if isFetchAllRegulars, to represent a wildcard query (all
                                            // static and regular columns are both _fetched_ and _queried_).
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

    private ColumnFilter(boolean isFetchAllRegulars,
                         CFMetaData metadata,
                         PartitionColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !isFetchAllRegulars || metadata != null;
        assert isFetchAllRegulars || queried != null;
        this.isFetchAllRegulars = isFetchAllRegulars;
        this.metadata = metadata;
        this.queried = queried;
        this.subSelections = subSelections;
    }

    /**
     * A filter that includes all columns for the provided table.
     */
    public static ColumnFilter all(CFMetaData metadata)
    {
        return new ColumnFilter(true, metadata, null, null);
    }

    /**
     * A filter that only fetches/queries the provided columns.
     * <p>
     * Note that this shouldn't be used for CQL queries in general as all columns should be queried to
     * preserve CQL semantic (see class javadoc). This is ok for some internal queries however (and
     * for #6588 if/when we implement it).
     */
    public static ColumnFilter selection(PartitionColumns columns)
    {
        return new ColumnFilter(false, null, columns, null);
    }

    /**
     * The columns that needs to be fetched internally for this filter.
     *
     * @return the columns to fetch for this filter.
     */
    public PartitionColumns fetchedColumns()
    {
        if (!isFetchAllRegulars)
            return queried;

        // We always fetch all regulars, but only fetch the statics in queried. Unless queried == null, in which
        // case it's a wildcard and we fetch everything.
        PartitionColumns all = metadata.partitionColumns();
        return queried == null || all.statics.isEmpty()
             ? all
             : new PartitionColumns(queried.statics, all.regulars);
    }

    /**
     * The columns actually queried by the user.
     * <p>
     * Note that this is in general not all the columns that are fetched internally (see {@link #fetchedColumns}).
     */
    public PartitionColumns queriedColumns()
    {
        assert queried != null || isFetchAllRegulars;
        return queried == null ? metadata.partitionColumns() : queried;
    }

    /**
     * Wether all the (regular or static) columns are fetched by this filter.
     * <p>
     * Note that this method is meant as an optimization but a negative return
     * shouldn't be relied upon strongly: this can return {@code false} but
     * still have all the columns fetches if those were manually selected by the
     * user. The goal here is to cheaply avoid filtering things on wildcard
     * queries, as those are common.
     *
     * @param isStatic whether to check for static columns or not. If {@code true},
     * the method returns if all static columns are fetched, otherwise it checks
     * regular columns.
     */
    public boolean fetchesAllColumns(boolean isStatic)
    {
        return isStatic ? queried == null : isFetchAllRegulars;
    }

    /**
     * Whether _fetched_ == _queried_ for this filter, and so if the {@code isQueried()} methods
     * can return {@code false} for some column/cell.
     */
    public boolean allFetchedColumnsAreQueried()
    {
        return !isFetchAllRegulars || queried == null;
    }

    /**
     * Whether the provided column is fetched by this filter.
     */
    public boolean fetches(ColumnDefinition column)
    {
        // For statics, it is included only if it's part of _queried_, or if _queried_ is null (wildcard query).
        if (column.isStatic())
            return queried == null || queried.contains(column);

        // For regulars, if 'isFetchAllRegulars', then it's included automatically. Otherwise, it depends on _queried_.
        return isFetchAllRegulars || queried.contains(column);
    }

    /**
     * Whether the provided column, which is assumed to be _fetched_ by this filter (so the caller must guarantee
     * that {@code fetches(column) == true}, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedColumnIsQueried(ColumnDefinition column)
    {
        return !isFetchAllRegulars || queried == null || queried.contains(column);
    }

    /**
     * Whether the provided complex cell (identified by its column and path), which is assumed to be _fetched_ by
     * this filter, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedCellIsQueried(ColumnDefinition column, CellPath path)
    {
        assert path != null;
        if (!isFetchAllRegulars || subSelections == null)
            return true;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        // No subsection for this column means everything is queried
        if (s.isEmpty())
            return true;

        for (ColumnSubselection subSel : s)
            if (subSel.compareInclusionOf(path) == 0)
                return true;

        return false;
    }

    /**
     * Creates a new {@code Tester} to efficiently test the inclusion of cells of complex column
     * {@code column}.
     *
     * @param column for complex column for which to create a tester.
     * @return the created tester or {@code null} if all the cells from the provided column
     * are queried.
     */
    public Tester newTester(ColumnDefinition column)
    {
        if (subSelections == null || !column.isComplex())
            return null;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return null;

        return new Tester(!column.isStatic() && isFetchAllRegulars, s.iterator());
    }

    /**
     * Returns a {@code ColumnFilter}} builder that fetches all regular columns (and queries the columns
     * added to the builder, or everything if no column is added).
     */
    public static Builder allRegularColumnsBuilder(CFMetaData metadata)
    {
        return new Builder(metadata);
    }

    /**
     * Returns a {@code ColumnFilter} builder that only fetches the columns/cells added to the builder.
     */
    public static Builder selectionBuilder()
    {
        return new Builder(null);
    }

    public static class Tester
    {
        private final boolean isFetched;
        private ColumnSubselection current;
        private final Iterator<ColumnSubselection> iterator;

        private Tester(boolean isFetched, Iterator<ColumnSubselection> iterator)
        {
            this.isFetched = isFetched;
            this.iterator = iterator;
        }

        public boolean fetches(CellPath path)
        {
            return isFetched || hasSubselection(path);
        }

        /**
         * Must only be called if {@code fetches(path) == true}.
         */
        public boolean fetchedCellIsQueried(CellPath path)
        {
            return !isFetched || hasSubselection(path);
        }

        private boolean hasSubselection(CellPath path)
        {
            while (current != null || iterator.hasNext())
            {
                if (current == null)
                    current = iterator.next();

                int cmp = current.compareInclusionOf(path);
                if (cmp == 0) // The path is included
                    return true;
                else if (cmp < 0) // The path is before this sub-selection, it's not included by any
                    return false;

                // the path is after this sub-selection, we need to check the next one.
                current = null;
            }
            return false;
        }
    }

    /**
     * A builder for a {@code ColumnFilter} object.
     *
     * Note that the columns added to this build are the _queried_ column. Whether or not all columns
     * are _fetched_ depends on which constructor you've used to obtained this builder, allColumnsBuilder (all
     * columns are fetched) or selectionBuilder (only the queried columns are fetched).
     *
     * Note that for a allColumnsBuilder, if no queried columns are added, this is interpreted as querying
     * all columns, not querying none (but if you know you want to query all columns, prefer
     * {@link ColumnFilter#all(CFMetaData)}. For selectionBuilder, adding no queried columns means no column will be
     * fetched (so the builder will return {@code PartitionColumns.NONE}).
     */
    public static class Builder
    {
        private final CFMetaData metadata; // null if we don't fetch all columns
        private PartitionColumns.Builder queriedBuilder;
        private List<ColumnSubselection> subSelections;

        private Builder(CFMetaData metadata)
        {
            this.metadata = metadata;
        }

        public Builder add(ColumnDefinition c)
        {
            if (queriedBuilder == null)
                queriedBuilder = PartitionColumns.builder();
            queriedBuilder.add(c);
            return this;
        }

        public Builder addAll(Iterable<ColumnDefinition> columns)
        {
            if (queriedBuilder == null)
                queriedBuilder = PartitionColumns.builder();
            queriedBuilder.addAll(columns);
            return this;
        }

        private Builder addSubSelection(ColumnSubselection subSelection)
        {
            add(subSelection.column());
            if (subSelections == null)
                subSelections = new ArrayList<>();
            subSelections.add(subSelection);
            return this;
        }

        public Builder slice(ColumnDefinition c, CellPath from, CellPath to)
        {
            return addSubSelection(ColumnSubselection.slice(c, from, to));
        }

        public Builder select(ColumnDefinition c, CellPath elt)
        {
            return addSubSelection(ColumnSubselection.element(c, elt));
        }

        public ColumnFilter build()
        {
            boolean isFetchAll = metadata != null;

            PartitionColumns queried = queriedBuilder == null ? null : queriedBuilder.build();
            // It's only ok to have queried == null in ColumnFilter if isFetchAll. So deal with the case of a selectionBuilder
            // with nothing selected (we can at least happen on some backward compatible queries - CASSANDRA-10471).
            if (!isFetchAll && queried == null)
                queried = PartitionColumns.NONE;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = null;
            if (subSelections != null)
            {
                s = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                for (ColumnSubselection subSelection : subSelections)
                    s.put(subSelection.column().name, subSelection);
            }

            return new ColumnFilter(isFetchAll, metadata, queried, s);
        }
    }

    @Override
    public String toString()
    {
        if (isFetchAllRegulars && queried == null)
            return "*";

        if (queried.isEmpty())
            return "";

        Iterator<ColumnDefinition> defs = queried.selectOrderIterator();
        if (!defs.hasNext())
            return "<none>";

        StringBuilder sb = new StringBuilder();
        while (defs.hasNext())
        {
            appendColumnDef(sb, defs.next());
            if (defs.hasNext())
                sb.append(", ");
        }
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, ColumnDefinition column)
    {
        if (subSelections == null)
        {
            sb.append(column.name);
            return;
        }

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
        {
            sb.append(column.name);
            return;
        }

        int i = 0;
        for (ColumnSubselection subSel : s)
            sb.append(i++ == 0 ? "" : ", ").append(column.name).append(subSel);
    }

    public static class Serializer
    {
        private static final int IS_FETCH_ALL_MASK       = 0x01;
        private static final int HAS_QUERIED_MASK      = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;

        private static int makeHeaderByte(ColumnFilter selection)
        {
            return (selection.isFetchAllRegulars ? IS_FETCH_ALL_MASK : 0)
                 | (selection.queried != null ? HAS_QUERIED_MASK : 0)
                 | (selection.subSelections != null ? HAS_SUB_SELECTIONS_MASK : 0);
        }

        public void serialize(ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(makeHeaderByte(selection));

            if (selection.queried != null)
            {
                Columns.serializer.serialize(selection.queried.statics, out);
                Columns.serializer.serialize(selection.queried.regulars, out);
            }

            if (selection.subSelections != null)
            {
                out.writeUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    ColumnSubselection.serializer.serialize(subSel, out, version);
            }
        }

        public ColumnFilter deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
        {
            int header = in.readUnsignedByte();
            boolean isFetchAll = (header & IS_FETCH_ALL_MASK) != 0;
            boolean hasQueried = (header & HAS_QUERIED_MASK) != 0;
            boolean hasSubSelections = (header & HAS_SUB_SELECTIONS_MASK) != 0;

            PartitionColumns queried = null;
            if (hasQueried)
            {
                Columns statics = Columns.serializer.deserialize(in, metadata);
                Columns regulars = Columns.serializer.deserialize(in, metadata);
                queried = new PartitionColumns(statics, regulars);
            }

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
            if (hasSubSelections)
            {
                subSelections = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                int size = (int)in.readUnsignedVInt();
                for (int i = 0; i < size; i++)
                {
                    ColumnSubselection subSel = ColumnSubselection.serializer.deserialize(in, version, metadata);
                    subSelections.put(subSel.column().name, subSel);
                }
            }

            return new ColumnFilter(isFetchAll, isFetchAll ? metadata : null, queried, subSelections);
        }

        public long serializedSize(ColumnFilter selection, int version)
        {
            long size = 1; // header byte

            if (selection.queried != null)
            {
                size += Columns.serializer.serializedSize(selection.queried.statics);
                size += Columns.serializer.serializedSize(selection.queried.regulars);
            }

            if (selection.subSelections != null)
            {

                size += TypeSizes.sizeofUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    size += ColumnSubselection.serializer.serializedSize(subSel, version);
            }

            return size;
        }
    }
}
