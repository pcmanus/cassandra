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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * Groups all the options of a {@code SelectStatement}.
 * <p>
 * The reasons this is separated from {@link SelectStatement} is twofold:
 *  1) {@code SelectStatement} is fairly big and that class gives it a bit of breathing room,
 *     handling some of the breathing room in particular.
 *  2) some data (currently the {@code ColumnFilter} and the concrete limits) cannot always
 *     be computed at preparation time (they can depend on bind markers) but it is common that
 *     they can (be computed at preparation time) and we want to do that when possible. This
 *     class "abstract" that optimization (through the {@code Data} private subclass).
 */
public class SelectOptions
{
    private final CFMetaData metadata;
    private final Selection selection;
    private final StatementRestrictions restrictions;
    private final SelectStatement.Parameters parameters;
    private final boolean isReversed;

    private final Term limit;
    private final Term perPartitionLimit;

    private final Data data;

    private SelectOptions(CFMetaData metadata,
                          Selection selection,
                          StatementRestrictions restrictions,
                          SelectStatement.Parameters parameters,
                          boolean isReversed,
                          Term limit,
                          Term perPartitionLimit,
                          Data data)
    {
        this.metadata = metadata;
        this.selection = selection;
        this.restrictions = restrictions;
        this.parameters = parameters;
        this.isReversed = isReversed;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
        this.data = data;
    }

    /**
     * Creates a {@code SelectOptions}.
     *
     * @param metadata the metadata for the table the SELECT is on.
     * @param selection the {@code Selection} of the SELECT (the part between SELECT and FROM).
     * @param restrictions the restrictions (WHERE clause) of the SELECT.
     * @param parameters some of the parameters of the SELECT (the ORDER BY if any, whether
     * ALLOW FILTERING is used, ...)
     * @param isReversed whether the overall query is in reversed clustering order or not.
     * @param limit the user LIMIT of the query.
     * @param perPartitionLimit the user PER PARTITION LIMIT of the query.
     * @return the created {@code SelectOptions}.
     */
    public static SelectOptions create(CFMetaData metadata,
                                       Selection selection,
                                       StatementRestrictions restrictions,
                                       SelectStatement.Parameters parameters,
                                       boolean isReversed,
                                       Term limit,
                                       Term perPartitionLimit)
    {
        // Check if we can compute the ColumnFilter now, which we can do if the selection is "terminal"
        ColumnFilter columnFilter = null;
        if (selection.isWildcard())
        {
            columnFilter = ColumnFilter.all(metadata);
        }
        else if (selection.isTerminal())
        {
            ColumnFilter.Builder builder = ColumnFilter.allColumnsBuilder(metadata);
            selection.addFetchedColumns(builder);
            // we'll also need to fetch any column on which we have a restriction (so we can apply said restriction)
            builder.addAll(restrictions.nonPKRestrictedColumns(true));
            columnFilter = builder.build();
        }
        Data data = new Data(columnFilter, maybeGetLimit(limit), maybeGetLimit(perPartitionLimit));

        return new SelectOptions(metadata,
                                 selection,
                                 restrictions,
                                 parameters,
                                 isReversed,
                                 limit,
                                 perPartitionLimit,
                                 data);
    }

    /**
     * The metadata for the table the SELECT is on.
     */
    public CFMetaData metadata()
    {
        return metadata;
    }

    /**
     * The {@code Selection} of the SELECT (the part between SELECT and FROM).
     */
    public Selection selection()
    {
        return selection;
    }

    /**
     * The restrictions (WHERE clause) of the SELECT.
     */
    public StatementRestrictions restrictions()
    {
        return restrictions;
    }

    /**
     * The parameters of the SELECT (the ORDER BY if any, whether
     */
    public SelectStatement.Parameters parameters()
    {
        return parameters;
    }

    /**
     * "Binds" those SELECT options to a given execution context (query options).
     * <p>
     * This is a shortcut for {@code finalize(options, FBUtilities.nowInSeconds())}.
     *
     * @param options the options of the query being executed (mostly, the bind values).
     * @return the created {@code Finalized} object.
     */
    public Finalized finalize(QueryOptions options)
    {
        return finalize(options, FBUtilities.nowInSeconds());
    }

    /**
     * "Binds" those SELECT options to a given execution context (query options).
     *
     * @param options the options of the query being executed (mostly, the bind values).
     * @param nowInSec the time in seconds to use as "now" for this query.
     * @return the created {@code Finalized} object.
     */
    public Finalized finalize(QueryOptions options, int nowInSec)
    {
        Selection.Finalized finalizedSelection = selection.finalize(options);

        if (data.isTerminal())
            return new Finalized(this, options, nowInSec, finalizedSelection, data);

        // Some of data depends on bind values

        ColumnFilter columnFilter = data.columnFilter;
        if (columnFilter == null)
        {
            ColumnFilter.Builder builder = ColumnFilter.allColumnsBuilder(metadata);
            finalizedSelection.addFetchedColumns(builder);
            // we'll also need to fetch any column on which we have a restriction (so we can apply said restriction)
            builder.addAll(restrictions.nonPKRestrictedColumns(true));
            columnFilter = builder.build();
        }

        int userLimit = data.limit < 0 ? getLimit(limit, options) : data.limit;
        int userPerPartitionLimit = data.perPartitionLimit < 0 ? getLimit(perPartitionLimit, options) : data.perPartitionLimit;
        return new Finalized(this,
                             options,
                             nowInSec,
                             finalizedSelection,
                             new Data(columnFilter, userLimit, userPerPartitionLimit));
    }

    /**
     * Adds to the provided list all the functions used by the SELECT.
     *
     * @param functions a list to which to add the functions used by the SELECT
     * having those options.
     */
    public void addFunctionsTo(List<Function> functions)
    {
        selection.addFunctionsTo(functions);
        restrictions.addFunctionsTo(functions);

        if (limit != null)
            limit.addFunctionsTo(functions);

        if (perPartitionLimit != null)
            perPartitionLimit.addFunctionsTo(functions);
    }


    /**
     * Whether the SELECT having these options require post-query ordering of the result set.
     */
    public boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty();
    }

    private static int maybeGetLimit(Term limit)
    {
        if (limit == null)
            return DataLimits.NO_LIMIT;

        if (!limit.isTerminal())
            return -1;

        return getLimit(((Term.Terminal)limit).get(Server.VERSION_3));
    }

    private static int getLimit(Term limit, QueryOptions options)
    {
        ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");
        return b == ByteBufferUtil.UNSET_BYTE_BUFFER ? DataLimits.NO_LIMIT : getLimit(b);
    }

    private static int getLimit(ByteBuffer b)
    {
        try
        {
            Int32Type.instance.validate(b);
            int userLimit = Int32Type.instance.compose(b);
            checkTrue(userLimit > 0, "LIMIT must be strictly positive");
            return userLimit;
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid limit value");
        }
    }

    /**
     * The result of binding the options of a SELECT statement to bound values.
     * <p>
     * Where {@code SelectOptions} are computed at preparation, a {@code Finalized} object
     * is specific to a single execution.
     */
    public static class Finalized
    {
        private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;

        private final SelectOptions selectOptions;
        private final QueryOptions queryOptions;
        private final int nowInSec;

        private final Selection.Finalized selection;
        private final Data data;

        private Finalized(SelectOptions selectOptions, QueryOptions queryOptions, int nowInSec, Selection.Finalized selection, Data data)
        {
            this.selectOptions = selectOptions;
            this.queryOptions = queryOptions;
            this.nowInSec = nowInSec;

            this.selection = selection;
            this.data = data;

            assert data.isTerminal();
        }

        public CFMetaData metadata()
        {
            return selectOptions.metadata;
        }

        public ColumnFilter columnFilter()
        {
            return data.columnFilter;
        }

        public int limit()
        {
            return data.limit;
        }

        public int perPartitionLimit()
        {
            return data.perPartitionLimit;
        }

        public int nowInSec()
        {
            return nowInSec;
        }

        public int pageSize()
        {
            int pageSize = queryOptions.getPageSize();

            // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
            // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
            // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
            if (isAggregate() && pageSize <= 0)
                pageSize = DEFAULT_COUNT_PAGE_SIZE;

            return pageSize;
        }

        public boolean isPartitionRangeSelect()
        {
            // Note that forcing a partition range query when we use 2ndary indexes is largely
            // technical debt and will be "fixed" eventually, see CASSANDRA-11872.
            return selectOptions.restrictions.isKeyRange() || selectOptions.restrictions.usesSecondaryIndexing();
        }

        public QueryOptions queryOptions()
        {
            return queryOptions;
        }

        public boolean isAggregate()
        {
            return selectOptions.selection.isAggregate();
        }

        public StatementRestrictions restrictions()
        {
            return selectOptions.restrictions;
        }

        public Selection.ResultSetBuilder resultSetBuilder()
        {
            return selection.resultSetBuilder(queryOptions, selectOptions.parameters.isJson);
        }

        public Collection<ByteBuffer> makePartitionKeys()
        {
            return restrictions().getPartitionKeys(queryOptions);
        }

        public AbstractBounds<PartitionPosition> makePartitionKeyBounds()
        {
            return restrictions().getPartitionKeyBounds(queryOptions);
        }

        public DataLimits getDataLimits()
        {
            int cqlRowLimit = DataLimits.NO_LIMIT;
            int cqlPerPartitionLimit = DataLimits.NO_LIMIT;

            // If we aggregate, the limit really apply to the number of rows returned to the user, not to what is queried, and
            // since in practice we currently only aggregate at top level (we have no GROUP BY support yet), we'll only ever
            // return 1 result and can therefore basically ignore the user LIMIT in this case.
            // Whenever we support GROUP BY, we'll have to add a new DataLimits kind that knows how things are grouped and is thus
            // able to apply the user limit properly.
            // If we do post ordering we need to get all the results sorted before we can trim them.
            if (!isAggregate())
            {
                if (!selectOptions.needsPostQueryOrdering())
                    cqlRowLimit = limit();
                cqlPerPartitionLimit = perPartitionLimit();
            }
            if (selectOptions.parameters().isDistinct)
                return cqlRowLimit == DataLimits.NO_LIMIT ? DataLimits.DISTINCT_NONE : DataLimits.distinctLimits(cqlRowLimit);

            return DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit);
        }

        /**
         * Computes the {@code ClusteringIndexNamesFilter} for this SELECT query.
         */
        public ClusteringIndexFilter makeClusteringIndexFilter() throws InvalidRequestException
        {
            if (selectOptions.parameters.isDistinct)
            {
                // We need to be able to distinguish between partition having live rows and those that don't. But
                // doing so is not trivial since "having a live row" depends potentially on
                //   1) when the query is performed, due to TTLs
                //   2) how thing reconcile together between different nodes
                // so that it's hard to really optimize properly internally. So to keep it simple, we simply query
                // for the first row of the partition and hence uses Slices.ALL. We'll limit it to the first live
                // row however in getLimit().
                return new ClusteringIndexSliceFilter(Slices.ALL, false);
            }

            if (restrictions().isColumnRange())
            {
                Slices slices = makeSlices();
                if (slices == Slices.NONE && !selectOptions.selection.containsStaticColumns())
                    return null;

                return new ClusteringIndexSliceFilter(slices, selectOptions.isReversed);
            }
            else
            {
                NavigableSet<Clustering> clusterings = restrictions().getClusteringColumns(queryOptions);
                // We can have no clusterings if either we're only selecting the static columns, or if we have
                // a 'IN ()' for clusterings. In that case, we still want to query if some static columns are
                // fetched. But we're fine otherwise.
                if (clusterings.isEmpty() && data.columnFilter.fetchedColumns().statics.isEmpty())
                    return null;

                return new ClusteringIndexNamesFilter(clusterings, selectOptions.isReversed);
            }
        }

        private Slices makeSlices() throws InvalidRequestException
        {
            SortedSet<ClusteringBound> startBounds = restrictions().getClusteringColumnsBounds(Bound.START, queryOptions);
            SortedSet<ClusteringBound> endBounds = restrictions().getClusteringColumnsBounds(Bound.END, queryOptions);
            assert startBounds.size() == endBounds.size();

            ClusteringComparator comparator = metadata().comparator;

            // The case where startBounds == 1 is common enough that it's worth optimizing
            if (startBounds.size() == 1)
            {
                ClusteringBound start = startBounds.first();
                ClusteringBound end = endBounds.first();
                return comparator.compare(start, end) > 0
                     ? Slices.NONE
                     : Slices.with(comparator, Slice.make(start, end));
            }

            Slices.Builder builder = new Slices.Builder(comparator, startBounds.size());
            Iterator<ClusteringBound> startIter = startBounds.iterator();
            Iterator<ClusteringBound> endIter = endBounds.iterator();
            while (startIter.hasNext() && endIter.hasNext())
            {
                ClusteringBound start = startIter.next();
                ClusteringBound end = endIter.next();

                // Ignore slices that are nonsensical
                if (comparator.compare(start, end) > 0)
                    continue;

                builder.add(start, end);
            }

            return builder.build();
        }
    }

    /**
     * A simple utility class that groups data that can sometimes be computed at preparation and might
     * not always be. If they can be computed at preparation, they will be and will be reused directly
     * in a {@code Finalized} object, otherwise they will be computed at each execution (by {@link #finalize()}).
     */
    private static class Data
    {
        // This can be null within a SelectOptions (if computing the filter depends on bound values), but not within a Finalized.
        private final ColumnFilter columnFilter;

        // Both can be < 0 within a SelectOptions (if computing them depends on bound values), but not within a Finalized.
        private final int limit;
        private final int perPartitionLimit;

        private Data(ColumnFilter columnFilter,
                     int limit,
                     int perPartitionLimit)
        {
            this.columnFilter = columnFilter;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        private boolean isTerminal()
        {
            return columnFilter != null && limit >= 0 && perPartitionLimit >= 0;
        }
    }
}
