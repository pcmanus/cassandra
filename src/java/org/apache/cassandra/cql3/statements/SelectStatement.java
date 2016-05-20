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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 * A number of public methods here are only used internally. However,
 * many of these are made accessible for the benefit of custom
 * QueryHandler implementations, so before reducing their accessibility
 * due consideration should be given.
 */
public class SelectStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private final int boundTerms;

    private final SelectOptions selectOptions;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    private final Comparator<List<ByteBuffer>> orderingComparator;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnDefinition.Raw, Boolean>emptyMap(), false, false, false);

    public SelectStatement(CFMetaData cfm,
                           int boundTerms,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           Comparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.selectOptions = SelectOptions.create(cfm, selection, restrictions, parameters, isReversed, limit, perPartitionLimit);
        this.boundTerms = boundTerms;
        this.orderingComparator = orderingComparator;
    }

    public CFMetaData metadata()
    {
        return selectOptions.metadata();
    }

    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        selectOptions.addFunctionsTo(functions);
        return functions;
    }

    /**
     * The {@link SelectOptions} for this SELECT statement.
     * Exposed for the sake of QueryHandlers.
     */
    public SelectOptions options()
    {
        return selectOptions;
    }

    /**
     * The columns to fetch internally for this SELECT statement (which can be more than the one selected by the
     * user as it also include any restricted column in particular).
     */
    public ColumnFilter fetchedColumns()
    {
        return selectOptions.finalize(QueryOptions.DEFAULT).columnFilter();
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm,
                                   0,
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(StatementType.SELECT, cfm),
                                   false,
                                   null,
                                   null,
                                   null);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return selectOptions.selection().getResultMetadata(selectOptions.parameters().isJson);
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (metadata().isView())
        {
            CFMetaData baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.hasColumnFamilyAccess(baseTable, Permission.SELECT);
        }
        else
        {
            state.hasColumnFamilyAccess(metadata(), Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions queryOptions) throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = queryOptions.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead(keyspace());
        SelectOptions.Finalized options = selectOptions.finalize(queryOptions);

        ReadQuery query = getQuery(options);
        int pageSize = options.pageSize();

        if (pageSize <= 0 || query.limits().count() <= pageSize)
            return execute(query, options, state);

        QueryPager pager = query.getPager(queryOptions.getPagingState(), queryOptions.getProtocolVersion());
        return execute(Pager.forDistributedQuery(pager, cl, state.getClientState()), options);
    }

    public ReadQuery getQuery(QueryOptions options, int nowInSec) throws RequestValidationException
    {
        return getQuery(selectOptions.finalize(options, nowInSec));
    }

    private ReadQuery getQuery(SelectOptions.Finalized options) throws RequestValidationException
    {
        ClusteringIndexFilter filter = options.makeClusteringIndexFilter();
        if (filter == null)
            return ReadQuery.EMPTY;

        return options.isPartitionRangeSelect()
             ? getRangeCommand(options, filter)
             : getSliceCommands(options, filter);
    }

    private ResultMessage.Rows execute(ReadQuery query,
                                       SelectOptions.Finalized options,
                                       QueryState state) throws RequestValidationException, RequestExecutionException
    {
        try (PartitionIterator data = query.execute(options.queryOptions().getConsistency(), state.getClientState()))
        {
            return processResults(data, options);
        }
    }

    // Simple wrapper class to avoid some code duplication
    private static abstract class Pager
    {
        protected QueryPager pager;

        protected Pager(QueryPager pager)
        {
            this.pager = pager;
        }

        public static Pager forInternalQuery(QueryPager pager, ReadExecutionController executionController)
        {
            return new InternalPager(pager, executionController);
        }

        public static Pager forDistributedQuery(QueryPager pager, ConsistencyLevel consistency, ClientState clientState)
        {
            return new NormalPager(pager, consistency, clientState);
        }

        public boolean isExhausted()
        {
            return pager.isExhausted();
        }

        public PagingState state()
        {
            return pager.state();
        }

        public abstract PartitionIterator fetchPage(int pageSize);

        public static class NormalPager extends Pager
        {
            private final ConsistencyLevel consistency;
            private final ClientState clientState;

            private NormalPager(QueryPager pager, ConsistencyLevel consistency, ClientState clientState)
            {
                super(pager);
                this.consistency = consistency;
                this.clientState = clientState;
            }

            public PartitionIterator fetchPage(int pageSize)
            {
                return pager.fetchPage(pageSize, consistency, clientState);
            }
        }

        public static class InternalPager extends Pager
        {
            private final ReadExecutionController executionController;

            private InternalPager(QueryPager pager, ReadExecutionController executionController)
            {
                super(pager);
                this.executionController = executionController;
            }

            public PartitionIterator fetchPage(int pageSize)
            {
                return pager.fetchPageInternal(pageSize, executionController);
            }
        }
    }

    private ResultMessage.Rows execute(Pager pager, SelectOptions.Finalized options) throws RequestValidationException, RequestExecutionException
    {
        if (options.isAggregate())
            return pageAggregateQuery(pager, options);

        // We can't properly do post-query ordering if we page (see #6722)
        checkFalse(selectOptions.needsPostQueryOrdering(),
                  "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                  + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        ResultMessage.Rows msg;
        try (PartitionIterator page = pager.fetchPage(options.pageSize()))
        {
            msg = processResults(page, options);
        }

        // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
        // shouldn't be moved inside the 'try' above.
        if (!pager.isExhausted())
            msg.result.metadata.setHasMorePages(pager.state());

        return msg;
    }

    private ResultMessage.Rows pageAggregateQuery(Pager pager, SelectOptions.Finalized options)
    throws RequestValidationException, RequestExecutionException
    {
        if (!options.restrictions().hasPartitionKeyRestrictions())
        {
            logger.warn("Aggregation query used without partition key");
            ClientWarn.instance.warn("Aggregation query used without partition key");
        }
        else if (options.restrictions().keyIsInRelation())
        {
            logger.warn("Aggregation query used on multiple partition keys (IN restriction)");
            ClientWarn.instance.warn("Aggregation query used on multiple partition keys (IN restriction)");
        }

        Selection.ResultSetBuilder result = options.resultSetBuilder();
        int pageSize = options.pageSize();
        while (!pager.isExhausted())
        {
            try (PartitionIterator iter = pager.fetchPage(pageSize))
            {
                while (iter.hasNext())
                {
                    try (RowIterator partition = iter.next())
                    {
                        processPartition(partition, result, options.nowInSec());
                    }
                }
            }
        }
        return new ResultMessage.Rows(result.build());
    }

    private ResultMessage.Rows processResults(PartitionIterator partitions, SelectOptions.Finalized options) throws RequestValidationException
    {
        ResultSet rset = process(partitions, options);
        return new ResultMessage.Rows(rset);
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions queryOptions) throws RequestExecutionException, RequestValidationException
    {
        SelectOptions.Finalized options = selectOptions.finalize(queryOptions);
        ReadQuery query = getQuery(options);
        int pageSize = options.pageSize();

        try (ReadExecutionController executionController = query.executionController())
        {
            if (pageSize <= 0 || query.limits().count() <= pageSize)
            {
                try (PartitionIterator data = query.executeInternal(executionController))
                {
                    return processResults(data, options);
                }
            }
            else
            {
                QueryPager pager = query.getPager(queryOptions.getPagingState(), queryOptions.getProtocolVersion());
                return execute(Pager.forInternalQuery(pager, executionController), options);
            }
        }
    }

    public ResultSet process(PartitionIterator partitions, int nowInSec) throws InvalidRequestException
    {
        return process(partitions, selectOptions.finalize(QueryOptions.DEFAULT));
    }

    public String keyspace()
    {
        return metadata().ksName;
    }

    public String columnFamily()
    {
        return metadata().cfName;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public Selection getSelection()
    {
        return selectOptions.selection();
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public StatementRestrictions getRestrictions()
    {
        return selectOptions.restrictions();
    }

    private ReadQuery getSliceCommands(SelectOptions.Finalized options, ClusteringIndexFilter clusteringFilter) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = options.makePartitionKeys();
        if (keys.isEmpty())
            return ReadQuery.EMPTY;

        RowFilter rowFilter = getRowFilter(options.queryOptions());
        DataLimits limit = options.getDataLimits();

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            DecoratedKey dk = metadata().decorateKey(ByteBufferUtil.clone(key));
            commands.add(SinglePartitionReadCommand.create(options.metadata(), options.nowInSec(), options.columnFilter(), rowFilter, limit, dk, clusteringFilter));
        }

        return new SinglePartitionReadCommand.Group(commands, limit);
    }

    /**
     * Returns the slices fetched by this SELECT, assuming an internal call (no bound values in particular).
     * <p>
     * Note that if the SELECT intrinsically selects rows by names, we convert them into equivalent slices for
     * the purpose of this method. This is used for MVs to restrict what needs to be read when we want to read
     * everything that could be affected by a given view (and so, if the view SELECT statement has restrictions
     * on the clustering columns, we can restrict what we read).
     */
    public Slices clusteringIndexFilterAsSlices()
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClusteringIndexFilter filter = selectOptions.finalize(options).makeClusteringIndexFilter();
        if (filter instanceof ClusteringIndexSliceFilter)
            return ((ClusteringIndexSliceFilter)filter).requestedSlices();

        Slices.Builder builder = new Slices.Builder(metadata().comparator);
        for (Clustering clustering: ((ClusteringIndexNamesFilter)filter).requestedRows())
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    /**
     * Returns a read command that can be used internally to query all the rows queried by this SELECT for a
     * give key (used for materialized views).
     */
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec)
    {
        SelectOptions.Finalized options = selectOptions.finalize(QueryOptions.forInternalCalls(Collections.emptyList()));
        ClusteringIndexFilter filter = options.makeClusteringIndexFilter();
        RowFilter rowFilter = getRowFilter(options.queryOptions());
        return SinglePartitionReadCommand.create(options.metadata(), options.nowInSec(), options.columnFilter(), rowFilter, DataLimits.NONE, key, filter);
    }

    /**
     * The {@code RowFilter} for this SELECT, assuming an internal call (no bound values in particular).
     */
    public RowFilter rowFilterForInternalCalls()
    {
        return getRowFilter(QueryOptions.forInternalCalls(Collections.emptyList()));
    }

    private ReadQuery getRangeCommand(SelectOptions.Finalized options, ClusteringIndexFilter clusteringFilter) throws RequestValidationException
    {
        RowFilter rowFilter = getRowFilter(options.queryOptions());

        AbstractBounds<PartitionPosition> keyBounds = options.makePartitionKeyBounds();
        if (keyBounds == null)
            return ReadQuery.EMPTY;

        PartitionRangeReadCommand command = new PartitionRangeReadCommand(options.metadata(),
                                                                          options.nowInSec(),
                                                                          options.columnFilter(),
                                                                          rowFilter,
                                                                          options.getDataLimits(),
                                                                          new DataRange(keyBounds, clusteringFilter),
                                                                          Optional.empty());
        // If there's a secondary index that the command can use, have it validate
        // the request parameters. Note that as a side effect, if a viable Index is
        // identified by the CFS's index manager, it will be cached in the command
        // and serialized during distribution to replicas in order to avoid performing
        // further lookups.
        command.maybeValidateIndex();

        return command;
    }

    /**
     * Returns the limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     *
     * @deprecated QueryHandler should directly obtain the {@link SelectOptions} with {@link #options()}
     * and call {@link SelectOptions#finalize()} manually to avoid finalizing multiple times.
     */
    @Deprecated
    public int getLimit(QueryOptions options)
    {
        return selectOptions.finalize(options).limit();
    }

    /**
     * Returns the per partition limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the per partition limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     *
     * @deprecated QueryHandler should directly obtain the {@link SelectOptions} with {@link #options()}
     * and call {@link SelectOptions#finalize()} manually to avoid finalizing multiple times.
     */
    @Deprecated
    public int getPerPartitionLimit(QueryOptions options)
    {
        return selectOptions.finalize(options).perPartitionLimit();
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public RowFilter getRowFilter(QueryOptions options) throws InvalidRequestException
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
        RowFilter filter = selectOptions.restrictions().getRowFilter(secondaryIndexManager, options);
        return filter;
    }

    private ResultSet process(PartitionIterator partitions, SelectOptions.Finalized options) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = options.resultSetBuilder();
        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, result, options.nowInSec());
            }
        }

        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        cqlRows.trim(options.limit());

        return cqlRows;
    }

    public static ByteBuffer[] getComponents(CFMetaData cfm, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            return ((CompositeType)cfm.getKeyValidator()).split(key);
        }
        else
        {
            return new ByteBuffer[]{ key };
        }
    }

    // Used by ModificationStatement for CAS operations
    void processPartition(RowIterator partition, Selection.ResultSetBuilder result, int nowInSec)
    throws InvalidRequestException
    {
        ByteBuffer[] keyComponents = getComponents(metadata(), partition.partitionKey());
        List<ColumnDefinition> selectedColumns = selectOptions.selection().getColumns();

        Row staticRow = partition.staticRow();
        // If there is no rows, and there's no restriction on clustering/regular columns,
        // then provided the select was a full partition selection (either by partition key and/or by static column),
        // we want to include static columns and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && (!selectOptions.restrictions().hasClusteringColumnsRestriction() || metadata().isStaticCompactTable()))
            {
                result.newRow();
                for (ColumnDefinition def : selectedColumns)
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                            result.add(keyComponents[def.position()]);
                            break;
                        case STATIC:
                            addValue(result, def, staticRow, nowInSec);
                            break;
                        default:
                            result.add((ByteBuffer)null);
                    }
                }
            }
            return;
        }

        while (partition.hasNext())
        {
            Row row = partition.next();
            result.newRow();
            // Respect selection order
            for (ColumnDefinition def : selectedColumns)
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        result.add(row.clustering().get(def.position()));
                        break;
                    case REGULAR:
                        addValue(result, def, row, nowInSec);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, nowInSec);
                        break;
                }
            }
        }
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, Row row, int nowInSec)
    {
        if (def.isComplex())
        {
            assert def.type.isMultiCell();
            ComplexColumnData complexData = row.getComplexColumnData(def);
            if (complexData == null)
                result.add(null);
            else if (def.type.isCollection())
                result.add(((CollectionType) def.type).serializeForNativeProtocol(complexData.iterator(), result.protocolVersion));
            else
                result.add(((UserType) def.type).serializeForNativeProtocol(complexData.iterator(), result.protocolVersion));
        }
        else
        {
            result.add(row.getCell(def), nowInSec);
        }
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !selectOptions.needsPostQueryOrdering())
            return;

        Collections.sort(cqlRows.rows, orderingComparator);
    }

    public static class RawStatement extends CFStatement
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;

        public RawStatement(CFName cfName, Parameters parameters,
                            List<RawSelector> selectClause,
                            WhereClause whereClause,
                            Term.Raw limit,
                            Term.Raw perPartitionLimit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            return prepare(false);
        }

        public ParsedStatement.Prepared prepare(boolean forView) throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            Selection selection = selectClause.isEmpty()
                                  ? Selection.wildcard(cfm)
                                  : Selection.fromSelectors(cfm, selectClause, boundNames);

            StatementRestrictions restrictions = prepareRestrictions(cfm, boundNames, selection, forView);

            if (parameters.isDistinct)
            {
                checkNull(perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
                validateDistinctSelection(cfm, selection, restrictions);
            }

            checkFalse(selection.isAggregate() && perPartitionLimit != null,
                       "PER PARTITION LIMIT is not allowed with aggregate queries.");

            Comparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!parameters.orderings.isEmpty())
            {
                assert !forView;
                verifyOrderingIsAllowed(restrictions);
                orderingComparator = getOrderingComparator(cfm, selection, restrictions);
                isReversed = isReversed(cfm);
                if (isReversed)
                    orderingComparator = Collections.reverseOrder(orderingComparator);
            }

            checkNeedsFiltering(restrictions);

            SelectStatement stmt = new SelectStatement(cfm,
                                                        boundNames.size(),
                                                        parameters,
                                                        selection,
                                                        restrictions,
                                                        isReversed,
                                                        orderingComparator,
                                                        prepareLimit(boundNames, limit, keyspace(), limitReceiver()),
                                                        prepareLimit(boundNames, perPartitionLimit, keyspace(), perPartitionLimitReceiver()));

            return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }

        /**
         * Prepares the restrictions.
         *
         * @param cfm the column family meta data
         * @param boundNames the variable specifications
         * @param selection the selection
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(CFMetaData cfm,
                                                          VariableSpecifications boundNames,
                                                          Selection selection,
                                                          boolean forView) throws InvalidRequestException
        {
            return new StatementRestrictions(StatementType.SELECT,
                                             cfm,
                                             whereClause,
                                             boundNames,
                                             selection.containsOnlyStaticColumns(),
                                             selection.containsAComplexColumn(),
                                             parameters.allowFiltering,
                                             forView);
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames, Term.Raw limit,
                                  String keyspace, ColumnSpecification limitReceiver) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace, limitReceiver);
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException
        {
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(CFMetaData cfm,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            checkFalse(restrictions.hasClusteringColumnsRestriction() ||
                       (restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnDefinition::isStatic)),
                       "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");

            Collection<ColumnDefinition> requestedColumns = selection.getColumns();
            for (ColumnDefinition def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnDefinition def : cfm.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        private Comparator<List<ByteBuffer>> getOrderingComparator(CFMetaData cfm,
                                                                   Selection selection,
                                                                   StatementRestrictions restrictions)
                                                                   throws InvalidRequestException
        {
            if (!restrictions.keyIsInRelation())
                return null;

            Map<ColumnIdentifier, Integer> orderingIndexes = getOrderingIndex(cfm, selection);

            List<Integer> idToSort = new ArrayList<Integer>();
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                ColumnDefinition orderingColumn = raw.prepare(cfm);
                idToSort.add(orderingIndexes.get(orderingColumn.name));
                sorters.add(orderingColumn.type);
            }
            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private Map<ColumnIdentifier, Integer> getOrderingIndex(CFMetaData cfm, Selection selection)
                throws InvalidRequestException
        {
            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
            // even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            Map<ColumnIdentifier, Integer> orderingIndexes = new HashMap<>();
            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                final ColumnDefinition def = raw.prepare(cfm);
                int index = selection.getResultSetIndex(def);
                if (index < 0)
                    index = selection.addColumnForOrdering(def);
                orderingIndexes.put(def.name, index);
            }
            return orderingIndexes;
        }

        private boolean isReversed(CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnDefinition.Raw, Boolean> entry : parameters.orderings.entrySet())
            {
                ColumnDefinition def = entry.getKey().prepare(cfm);
                boolean reversed = entry.getValue();

                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);

                checkTrue(i++ == def.position(),
                          "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");

                reversedMap[def.position()] = (reversed != def.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the row filter is not the identity
                checkFalse(restrictions.needFiltering(), StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            }
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private ColumnSpecification perPartitionLimitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", cfName)
                              .add("selectClause", selectClause)
                              .add("whereClause", whereClause)
                              .add("isDistinct", parameters.isDistinct)
                              .toString();
        }
    }

    public static class Parameters
    {
        // Public because CASSANDRA-9858
        public final Map<ColumnDefinition.Raw, Boolean> orderings;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(Map<ColumnDefinition.Raw, Boolean> orderings,
                          boolean isDistinct,
                          boolean allowFiltering,
                          boolean isJson)
        {
            this.orderings = orderings;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
            this.isJson = isJson;
        }
    }

    private static abstract class ColumnComparator<T> implements Comparator<T>
    {
        protected final int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue)
        {
            if (aValue == null)
                return bValue == null ? 0 : -1;

            return bValue == null ? 1 : comparator.compare(aValue, bValue);
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return compare(comparator, a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                int comparison = compare(type, a.get(columnPos), b.get(columnPos));

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
