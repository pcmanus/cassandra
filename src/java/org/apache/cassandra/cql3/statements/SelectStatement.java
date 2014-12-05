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
import java.io.IOException;
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.github.jamm.MemoryMeter;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.ClusteringPrefix.EOC;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement implements CQLStatement, MeasurableForPreparedCache
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;

    /** Restrictions on partitioning columns */
    private final Restriction[] keyRestrictions;

    /** Restrictions on clustering columns */
    private final Restriction[] columnRestrictions;

    /** Restrictions on non-primary key columns (i.e. secondary index restrictions) */
    private final Map<ColumnIdentifier, Restriction> metadataRestrictions = new HashMap<ColumnIdentifier, Restriction>();

    // All restricted columns not covered by the key or index filter
    private final Set<ColumnDefinition> restrictedColumns = new HashSet<ColumnDefinition>();

    private boolean isReversed;
    private boolean onToken;
    private boolean isKeyRange;
    private boolean keyIsInRelation;
    private boolean usesSecondaryIndexing;

    private Map<ColumnIdentifier, Integer> orderingIndexes;

    private boolean selectsStaticColumns;
    private boolean selectsOnlyStaticColumns;

    private final PartitionColumns selectedColumns;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier, Boolean>emptyMap(), false, false);

    public SelectStatement(CFMetaData cfm, int boundTerms, Parameters parameters, Selection selection, Term limit)
    {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.keyRestrictions = new Restriction[cfm.partitionKeyColumns().size()];
        this.columnRestrictions = new Restriction[cfm.clusteringColumns().size()];
        this.parameters = parameters;
        this.limit = limit;

        // Now gather a few info on whether we should bother with static columns or not for this statement
        this.selectedColumns = initColumnsInfo();
    }

    private PartitionColumns initColumnsInfo()
    {
        // If it's a wildcard, we do select static but not only them
        if (selection.isWildcard())
        {
            selectsStaticColumns = true;
            return cfm.partitionColumns();
        }

        // Otherwise, check the selected columns
        selectsStaticColumns = false;
        selectsOnlyStaticColumns = true;
        PartitionColumns.Builder builder = PartitionColumns.builder();
        for (ColumnDefinition def : selection.getColumns())
        {
            switch (def.kind)
            {
                case CLUSTERING_COLUMN:
                    selectsOnlyStaticColumns = false;
                    break;
                case STATIC:
                    selectsStaticColumns = true;
                    builder.add(def);
                    break;
                case REGULAR:
                case COMPACT_VALUE:
                    selectsOnlyStaticColumns = false;
                    builder.add(def);
                    break;
            }
        }
        return builder.build();
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm, 0, defaultParameters, selection, null);
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return selection.getResultMetadata();
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        return meter.measure(this)
             + meter.measureDeep(parameters)
             + meter.measureDeep(selection)
             + (limit == null ? 0 : meter.measureDeep(limit))
             + meter.measureDeep(keyRestrictions)
             + meter.measureDeep(columnRestrictions)
             + meter.measureDeep(metadataRestrictions)
             + meter.measureDeep(restrictedColumns)
             + (orderingIndexes == null ? 0 : meter.measureDeep(orderingIndexes));
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        cl.validateForRead(keyspace());

        DataLimits limit = getLimit(options);
        int nowInSec = FBUtilities.nowInSeconds();
        Pageable command = getPageableCommand(options, limit, nowInSec);

        int pageSize = options.getPageSize();

        // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
        if (selection.isAggregate() && pageSize <= 0)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        if (pageSize <= 0 || command == null || !QueryPagers.mayNeedPaging(command, pageSize))
            return execute(command, options, limit, nowInSec);

        QueryPager pager = QueryPagers.pager(command, cl, options.getPagingState());
        if (selection.isAggregate())
            return pageAggregateQuery(pager, options, pageSize, nowInSec);

        // We can't properly do post-query ordering if we page (see #6722)
        if (needsPostQueryOrdering())
            throw new InvalidRequestException("Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the "
                                            + "ORDER BY or the IN and sort client side, or disable paging for this query");

        try (DataIterator page = pager.fetchPage(pageSize))
        {
            ResultMessage.Rows msg = processResults(page, options, limit, nowInSec);

            if (!pager.isExhausted())
                msg.result.metadata.setHasMorePages(pager.state());

            return msg;
        }
    }

    private Pageable getPageableCommand(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        if (isKeyRange || usesSecondaryIndexing)
            return getRangeCommand(options, limit, nowInSec);

        List<SinglePartitionReadCommand> commands = getSliceCommands(options, limit, nowInSec);
        return commands == null ? null : new Pageable.ReadCommands(commands);
    }

    public Pageable getPageableCommand(QueryOptions options) throws RequestValidationException
    {
        return getPageableCommand(options, getLimit(options), FBUtilities.nowInSeconds());
    }

    private ResultMessage.Rows execute(Pageable command, QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException, RequestExecutionException
    {
        try (DataIterator data = command == null
                               ? DataIterators.EMPTY
                               : (command instanceof Pageable.ReadCommands
                                   ? StorageProxy.read(((Pageable.ReadCommands)command).commands, options.getConsistency())
                                   : StorageProxy.getRangeSlice((PartitionRangeReadCommand)command, options.getConsistency())))
        {
            return processResults(data, options, limit, nowInSec);
        }
    }

    private ResultMessage.Rows pageAggregateQuery(QueryPager pager, QueryOptions options, int pageSize, int nowInSec)
    throws RequestValidationException, RequestExecutionException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(nowInSec);
        while (!pager.isExhausted())
        {
            try (DataIterator iter = pager.fetchPage(pageSize))
            {
                while (iter.hasNext())
                    processPartition(iter.next(), options, nowInSec, result);
            }
        }
        return new ResultMessage.Rows(result.build());
    }

    private ResultMessage.Rows processResults(DataIterator partitions, QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        ResultSet rset = process(partitions, options, limit, nowInSec);
        return new ResultMessage.Rows(rset);
    }

    static DataIterator readLocally(List<SinglePartitionReadCommand> cmds, ColumnFamilyStore cfs, int nowInSec)
    {
        List<DataIterator> partitions = new ArrayList<>(cmds.size());
        for (ReadCommand cmd : cmds)
            partitions.add(PartitionIterators.asDataIterator(cmd.executeLocally(cfs), nowInSec));
        return DataIterators.concat(partitions);
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        DataLimits limit = getLimit(options);
        int nowInSec = FBUtilities.nowInSeconds();
        Pageable command = getPageableCommand(options, limit, nowInSec);
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(cfm);
        try (DataIterator data = command == null
                                ? DataIterators.EMPTY
                                : (command instanceof Pageable.ReadCommands
                                  ? readLocally(((Pageable.ReadCommands)command).commands, cfs, nowInSec)
                                  : PartitionIterators.asDataIterator(((PartitionRangeReadCommand)command).executeLocally(cfs), nowInSec)))
        {
            return processResults(data, options, limit, nowInSec);
        }
    }

    public ResultSet process(DataIterator partitions) throws InvalidRequestException
    {
        QueryOptions options = QueryOptions.DEFAULT;
        return process(partitions, options, getLimit(options), FBUtilities.nowInSeconds());
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    private List<SinglePartitionReadCommand> getSliceCommands(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = getKeys(options);
        if (keys.isEmpty()) // in case of IN () for (the last column of) the partition key.
            return null;

        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());

        PartitionFilter filter = makeFilter(options);
        if (filter == null)
            return null;

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            commands.add(ReadCommands.create(cfm, ByteBufferUtil.clone(key), filter, limit, nowInSec));
        }

        return commands;
    }

    private PartitionRangeReadCommand getRangeCommand(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        PartitionFilter partitionFilter = makeFilter(options);
        if (partitionFilter == null)
            return null;

        AbstractBounds<RowPosition> keyBounds = getKeyBounds(options);
        if (keyBounds == null)
            return null;

        ColumnFilter columnFilter = getValidatedIndexExpressions(options);
        return new PartitionRangeReadCommand(cfm, nowInSec, columnFilter, limit, new DataRange(keyBounds, partitionFilter));
    }

    private AbstractBounds<RowPosition> getKeyBounds(QueryOptions options) throws InvalidRequestException
    {
        IPartitioner<?> p = StorageService.getPartitioner();

        if (onToken)
        {
            Token startToken = getTokenBound(Bound.START, options, p);
            Token endToken = getTokenBound(Bound.END, options, p);

            boolean includeStart = includeKeyBound(Bound.START);
            boolean includeEnd = includeKeyBound(Bound.END);

            /*
             * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
             * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result
             * in that case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
             *
             * In practice, we want to return an empty result set if either startToken > endToken, or both are
             * equal but one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a)
             * or (a, a)). Note though that in the case where startToken or endToken is the minimum token, then
             * this special case rule should not apply.
             */
            int cmp = startToken.compareTo(endToken);
            if (!startToken.isMinimum() && !endToken.isMinimum() && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
                return null;

            RowPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
            RowPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

            return new Range<RowPosition>(start, end);
        }
        else
        {
            ByteBuffer startKeyBytes = getKeyBound(Bound.START, options);
            ByteBuffer finishKeyBytes = getKeyBound(Bound.END, options);

            RowPosition startKey = RowPosition.ForKey.get(startKeyBytes, p);
            RowPosition finishKey = RowPosition.ForKey.get(finishKeyBytes, p);

            if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
                return null;

            if (includeKeyBound(Bound.START))
            {
                return includeKeyBound(Bound.END)
                     ? new Bounds<RowPosition>(startKey, finishKey)
                     : new IncludingExcludingBounds<RowPosition>(startKey, finishKey);
            }
            else
            {
                return includeKeyBound(Bound.END)
                     ? new Range<RowPosition>(startKey, finishKey)
                     : new ExcludingBounds<RowPosition>(startKey, finishKey);
            }
        }
    }

    private boolean isAlwaysEmpty(ClusteringPrefix start, ClusteringPrefix end)
    {
        return cfm.comparator.compare(start, end) > 0;
    }

    private PartitionFilter makeFilter(QueryOptions options)
    throws InvalidRequestException
    {
        if (isColumnRange())
        {
            Slices slices = makeSlices(options);
            if (slices == Slices.NONE && !selectsStaticColumns)
                return null;

            return new SlicePartitionFilter(selectedColumns, slices, isReversed);
        }
        else
        {
            SortedSet<ClusteringPrefix> clusterings = getRequestedColumns(options);
            if (clusterings == null) // in case of IN () for the last column of the key
                return null;

            return new NamesPartitionFilter(selectedColumns, clusterings);
        }
    }

    private Slices makeSlices(QueryOptions options)
    throws InvalidRequestException
    {
        List<ClusteringPrefix> startBounds = getRequestedBound(Bound.START, options);
        List<ClusteringPrefix> endBounds = getRequestedBound(Bound.END, options);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1)
        {
            ClusteringPrefix start = startBounds.get(0);
            ClusteringPrefix end = endBounds.get(0);
            return cfm.comparator.compare(start, end) > 0
                 ? Slices.NONE
                 : Slices.make(cfm.comparator, start, end);
        }

        Slices.Builder builder = new Slices.Builder(cfm.comparator, startBounds.size());
        for (int i = 0; i < startBounds.size(); i++)
        {
            ClusteringPrefix start = startBounds.get(i);
            ClusteringPrefix end = endBounds.get(i);

            // Ignore slices that are nonsensical
            if (cfm.comparator.compare(start, end) > 0)
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    private DataLimits getLimit(QueryOptions options) throws InvalidRequestException
    {
        if (limit == null)
            return DataLimits.NONE;

        ByteBuffer b = limit.bindAndGet(options);
        if (b == null)
            throw new InvalidRequestException("Invalid null value of limit");

        try
        {
            Int32Type.instance.validate(b);
            int l = Int32Type.instance.compose(b);
            if (l <= 0)
                throw new InvalidRequestException("LIMIT must be strictly positive");

            return DataLimits.cqlLimits(l);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid limit value");
        }
    }

    public static ByteBuffer serializePartitionKey(ClusteringPrefix keyAsClusteringPrefix)
    {
        // TODO: we should stop using ClusteringPrefix for partition keys. Maybe we can add
        // a few methods to DecoratedKey so we don't have to (note that while using a ClusteringPrefix
        // allows to use buildBound(), it's actually used for partition keys only when every restriction
        // is an equal, so we could easily create a specific method for keys for that.
        if (keyAsClusteringPrefix.size() == 1)
            return keyAsClusteringPrefix.get(0);

        return LegacyLayout.serializeAsOldComposite(keyAsClusteringPrefix);
    }

    private Collection<ByteBuffer> getKeys(final QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        CBuilder builder = new CBuilder(cfm.getKeyValidatorAsClusteringComparator());
        for (ColumnDefinition def : cfm.partitionKeyColumns())
        {
            Restriction r = keyRestrictions[def.position()];
            assert r != null && !r.isSlice();

            List<ByteBuffer> values = r.values(options);

            if (builder.remainingCount() == 1)
            {
                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                    keys.add(serializePartitionKey(builder.buildWith(val)));
                }
            }
            else
            {
                // Note: for backward compatibility reasons, we let INs with 1 value slide
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                builder.add(val);
            }
        }
        return keys;
    }

    private ByteBuffer getKeyBound(Bound b, QueryOptions options) throws InvalidRequestException
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the first
        // component of a composite partition key).
        for (int i = 0; i < keyRestrictions.length; i++)
            if (keyRestrictions[i] == null)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        ClusteringPrefix prefix = buildBound(b, cfm.partitionKeyColumns(), keyRestrictions, cfm.getKeyValidatorAsClusteringComparator(), options, true).get(0);
        return prefix == EmptyClusteringPrefix.BOTTOM || prefix == EmptyClusteringPrefix.TOP
             ? ByteBufferUtil.EMPTY_BYTE_BUFFER
             : serializePartitionKey(prefix);
    }

    private Token getTokenBound(Bound b, QueryOptions options, IPartitioner<?> p) throws InvalidRequestException
    {
        assert onToken;

        Restriction restriction = keyRestrictions[0];

        assert !restriction.isMultiColumn() : "Unexpectedly got a multi-column restriction on a partition key for a range query";
        SingleColumnRestriction keyRestriction = (SingleColumnRestriction)restriction;

        ByteBuffer value;
        if (keyRestriction.isEQ())
        {
            value = keyRestriction.values(options).get(0);
        }
        else
        {
            SingleColumnRestriction.Slice slice = (SingleColumnRestriction.Slice)keyRestriction;
            if (!slice.hasBound(b))
                return p.getMinimumToken();

            value = slice.bound(b, options);
        }

        if (value == null)
            throw new InvalidRequestException("Invalid null token value");
        return p.getTokenFactory().fromByteArray(value);
    }

    private boolean includeKeyBound(Bound b)
    {
        for (Restriction r : keyRestrictions)
        {
            if (r == null)
                return true;
            else if (r.isSlice())
            {
                assert !r.isMultiColumn() : "Unexpectedly got multi-column restriction on partition key";
                return ((SingleColumnRestriction.Slice)r).isInclusive(b);
            }
        }
        // All equality
        return true;
    }

    private boolean isColumnRange()
    {
        // Due to CASSANDRA-5762, we always do a slice for CQL3 tables (not dense, composite).
        // Static CF (non dense but non composite) never entails a column slice however
        if (!cfm.layout().isDense())
            return cfm.layout().isCompound();

        // Otherwise (i.e. for compact table where we don't have a row marker anyway and thus don't care about CASSANDRA-5762),
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ.
        for (Restriction r : columnRestrictions)
        {
            if (r == null || r.isSlice())
                return true;
        }
        return false;
    }

    private SortedSet<ClusteringPrefix> getRequestedColumns(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !isColumnRange();

        MultiCBuilder builder = new MultiCBuilder(new CBuilder(cfm.comparator));
        Iterator<ColumnDefinition> idIter = cfm.clusteringColumns().iterator();
        for (Restriction r : columnRestrictions)
        {
            ColumnDefinition def = idIter.next();
            assert r != null && !r.isSlice();

            List<ByteBuffer> values = r.values(options);

            if (values.isEmpty())
                return null;

            builder.addEachElementToAll(values);
            if (builder.containsNull())
                throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s",
                                                                def.name));
        }

        return builder.build();
    }

    private boolean selectACollection()
    {
        for (ColumnDefinition def : selection.getColumns())
        {
            if (def.type instanceof CollectionType)
                return true;
        }

        return false;
    }

    private static List<ClusteringPrefix> buildBound(Bound bound,
                                                     List<ColumnDefinition> defs,
                                                     Restriction[] restrictions,
                                                     ClusteringComparator comparator,
                                                     QueryOptions options,
                                                     boolean isPartitionKey) throws InvalidRequestException
    {
        // No defs means no clustering columns (you can't have no partition
        // key) and so just select the whole partition (TODO: we should replace that by
        // a names query of the single row of this partition)
        if (defs.isEmpty())
        {
            assert !isPartitionKey;
            return Collections.<ClusteringPrefix>singletonList(bound == Bound.START ? EmptyClusteringPrefix.BOTTOM : EmptyClusteringPrefix.TOP);
        }

        CBuilder builder = new CBuilder(comparator);

        // check the first restriction to see if we're dealing with a multi-column restriction
        Restriction firstRestriction = restrictions[0];
        if (firstRestriction != null && firstRestriction.isMultiColumn())
        {
            if (firstRestriction.isSlice())
                return buildMultiColumnSliceBound(bound, defs, (MultiColumnRestriction.Slice) firstRestriction, builder, options);
            else if (firstRestriction.isIN())
                return buildMultiColumnInBound(bound, defs, (MultiColumnRestriction.IN) firstRestriction, builder, comparator, options);
            else
                return buildMultiColumnEQBound(bound, defs, (MultiColumnRestriction.EQ) firstRestriction, builder, options);
        }

        MultiCBuilder compositeBuilder = new MultiCBuilder(builder);

        for (ColumnDefinition def : defs)
        {
            Restriction r = restrictions[def.position()];
            // The bound of this method is refering to the clustering order. So if said clustering order
            // is reversed for this column, we should reverse the restriction we use.
            Bound b = isReversedType(def) ? Bound.reverse(bound) : bound;
            if (isNullRestriction(r, b))
            {
                assert !isPartitionKey;
                // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                // For composites, if there was preceding component and we're computing the end, we must change the last component
                // End-Of-Component, otherwise we would be selecting only one record.
                return new ArrayList<>(compositeBuilder.build(bound == Bound.START ? EOC.START : EOC.END));
            }

            if (r.isSlice())
            {
                assert !isPartitionKey; // Parition key slices are handled by getTokenBound
                compositeBuilder.addElementToAll(getSliceValue(r, b, options));
                Relation.Type relType = ((Restriction.Slice) r).getRelation(bound, b);
                return new ArrayList<>(compositeBuilder.build(eocForRelation(relType)));
            }

            compositeBuilder.addEachElementToAll(r.values(options));

            if (compositeBuilder.containsNull())
                throw new InvalidRequestException(
                        String.format("Invalid null clustering key part %s", def.name));
        }
        // Everything was an equal
        // Note that for partition keys, we should never use an EOC, as they don't make sense for the
        // partitioner (see #5240). But as partition key must always have all their components anyway,
        // that's ok.
        EOC eoc = isPartitionKey ? EOC.NONE : (bound == Bound.START ? EOC.START : EOC.END);
        return new ArrayList<>(compositeBuilder.build(eoc));
    }

    private static EOC eocForRelation(Relation.Type op)
    {
        switch (op)
        {
            case LT:
                // < X => using startOf(X) as finish bound
                return EOC.START;
            case GT:
            case LTE:
                // > X => using endOf(X) as start bound
                // <= X => using endOf(X) as finish bound
                return EOC.END;
            default:
                // >= X => using X as start bound (could use START_OF too)
                // = X => using X
                return EOC.NONE;
        }
    }

    private static List<ClusteringPrefix> buildMultiColumnSliceBound(Bound bound,
                                                                     List<ColumnDefinition> defs,
                                                                     MultiColumnRestriction.Slice slice,
                                                                     CBuilder builder,
                                                                     QueryOptions options) throws InvalidRequestException
    {
        Iterator<ColumnDefinition> iter = defs.iterator();
        ColumnDefinition firstName = iter.next();
        // A hack to preserve pre-6875 behavior for tuple-notation slices where the comparator mixes ASCENDING
        // and DESCENDING orders.  This stores the bound for the first component; we will re-use it for all following
        // components, even if they don't match the first component's reversal/non-reversal.  Note that this does *not*
        // guarantee correct query results, it just preserves the previous behavior.
        Bound firstComponentBound = isReversedType(firstName) ? Bound.reverse(bound) : bound;

        if (!slice.hasBound(firstComponentBound))
        {
            EOC eoc = builder.remainingCount() == 0 ? EOC.NONE : (bound == Bound.START ? EOC.START : EOC.END);
            return Collections.singletonList(builder.build(eoc));
        }

        List<ByteBuffer> vals = slice.componentBounds(firstComponentBound, options);

        ByteBuffer v = vals.get(firstName.position());
        if (v == null)
            throw new InvalidRequestException("Invalid null value in condition for column " + firstName.name);
        builder.add(v);

        while (iter.hasNext())
        {
            ColumnDefinition def = iter.next();
            if (def.position() >= vals.size())
                break;

            v = vals.get(def.position());
            if (v == null)
                throw new InvalidRequestException("Invalid null value in condition for column " + def.name);
            builder.add(v);
        }
        Relation.Type relType = slice.getRelation(bound, firstComponentBound);
        return Collections.singletonList(builder.build(eocForRelation(relType)));
    }

    private static List<ClusteringPrefix> buildMultiColumnInBound(Bound bound,
                                                                  List<ColumnDefinition> defs,
                                                                  MultiColumnRestriction.IN restriction,
                                                                  CBuilder builder,
                                                                  ClusteringComparator comparator,
                                                                  QueryOptions options) throws InvalidRequestException
    {
        List<List<ByteBuffer>> splitInValues = restriction.splitValues(options);

        // The IN query might not have listed the values in comparator order, so we need to re-sort
        // the bounds lists to make sure the slices works correctly (also, to avoid duplicates).
        TreeSet<ClusteringPrefix> inValues = new TreeSet<>(comparator);
        for (List<ByteBuffer> components : splitInValues)
        {
            for (int i = 0; i < components.size(); i++)
                if (components.get(i) == null)
                    throw new InvalidRequestException("Invalid null value in condition for column " + defs.get(i));

            EOC eoc = builder.remainingCount() - components.size() == 0 ? EOC.NONE : (bound == Bound.START ? EOC.START : EOC.END);
            inValues.add(builder.buildWith(components, eoc));
        }
        return new ArrayList<>(inValues);
    }

    private static List<ClusteringPrefix> buildMultiColumnEQBound(Bound bound,
                                                                  List<ColumnDefinition> defs,
                                                                  MultiColumnRestriction.EQ restriction,
                                                                  CBuilder builder,
                                                                  QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> values = restriction.values(options);
        for (int i = 0; i < values.size(); i++)
        {
            ByteBuffer component = values.get(i);
            if (component == null)
                throw new InvalidRequestException("Invalid null value in condition for column " + defs.get(i));
            builder.add(component);
        }

        EOC eoc = builder.remainingCount() == 0 ? EOC.NONE : (bound == Bound.START ? EOC.START : EOC.END);
        return Collections.singletonList(builder.build(eoc));
    }

    private static boolean isNullRestriction(Restriction r, Bound b)
    {
        return r == null || (r.isSlice() && !((Restriction.Slice)r).hasBound(b));
    }

    private static ByteBuffer getSliceValue(Restriction r, Bound b, QueryOptions options) throws InvalidRequestException
    {
        Restriction.Slice slice = (Restriction.Slice)r;
        assert slice.hasBound(b);
        ByteBuffer val = slice.bound(b, options);
        if (val == null)
            throw new InvalidRequestException(String.format("Invalid null clustering key part %s", r));
        return val;
    }

    private List<ClusteringPrefix> getRequestedBound(Bound b, QueryOptions options) throws InvalidRequestException
    {
        assert isColumnRange();
        return buildBound(b, cfm.clusteringColumns(), columnRestrictions, cfm.comparator, options, false);
    }

    public ColumnFilter getValidatedIndexExpressions(QueryOptions options) throws InvalidRequestException
    {
        if (!usesSecondaryIndexing || restrictedColumns.isEmpty())
            return ColumnFilter.NONE;

        ColumnFilter filter = new ColumnFilter();
        for (ColumnDefinition def : restrictedColumns)
        {
            Restriction restriction;
            switch (def.kind)
            {
                case PARTITION_KEY:
                    restriction = keyRestrictions[def.position()];
                    break;
                case CLUSTERING_COLUMN:
                    restriction = columnRestrictions[def.position()];
                    break;
                case REGULAR:
                case STATIC:
                    restriction = metadataRestrictions.get(def.name);
                    break;
                default:
                    // We don't allow restricting a COMPACT_VALUE for now in prepare.
                    throw new AssertionError();
            }

            if (restriction.isSlice())
            {
                Restriction.Slice slice = (Restriction.Slice)restriction;
                for (Bound b : Bound.values())
                {
                    if (slice.hasBound(b))
                    {
                        ByteBuffer value = validateIndexedValue(def, slice.bound(b, options));
                        filter.add(def, slice.getIndexOperator(b), value);
                    }
                }
            }
            else if (restriction.isContains())
            {
                SingleColumnRestriction.Contains contains = (SingleColumnRestriction.Contains)restriction;
                for (ByteBuffer value : contains.values(options))
                {
                    validateIndexedValue(def, value);
                    filter.add(def, ColumnFilter.Operator.CONTAINS, value);
                }
                for (ByteBuffer key : contains.keys(options))
                {
                    validateIndexedValue(def, key);
                    filter.add(def, ColumnFilter.Operator.CONTAINS_KEY, key);
                }
            }
            else
            {
                List<ByteBuffer> values = restriction.values(options);

                if (values.size() != 1)
                    throw new InvalidRequestException("IN restrictions are not supported on indexed columns");

                ByteBuffer value = validateIndexedValue(def, values.get(0));
                filter.add(def, ColumnFilter.Operator.EQ, value);
            }
        }
        
        if (usesSecondaryIndexing)
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
            SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
            secondaryIndexManager.validateIndexSearchersForQuery(filter);
        }
        
        return filter;
    }

    private static ByteBuffer validateIndexedValue(ColumnDefinition def, ByteBuffer value) throws InvalidRequestException
    {
        if (value == null)
            throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", def.name));
        if (value.remaining() > 0xFFFF)
            throw new InvalidRequestException("Index expression values may not be larger than 64K");
        return value;
    }

    private ResultSet process(DataIterator partitions, QueryOptions options, DataLimits limit, int nowInSec) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(nowInSec);
        while (partitions.hasNext())
            processPartition(partitions.next(), options, nowInSec, result);

        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        // Internal calls always return columns in the comparator order, even when reverse was set
        if (isReversed)
            cqlRows.reverse();

        // Trim result if needed to respect the user limit
        // TODO: I think we should remove that, we should never return more than asked anymore
        // If that's the case, we don't need the limit argument to this function
        //cqlRows.trim(limit.count());
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
    void processPartition(RowIterator partition, QueryOptions options, int nowInSec, Selection.ResultSetBuilder result)
    throws InvalidRequestException
    {
        try (RowIterator p = partition)
        {
            ByteBuffer[] keyComponents = getComponents(cfm, partition.partitionKey());

            Row staticRow = partition.staticRow().takeAlias();
            // If there is no atoms, then provided the select was a full partition selection
            // (i.e. not a 2ndary index search and there was no condition on clustering columns),
            // we want to include static columns and we're done.
            if (!partition.hasNext())
            {
                // We should have a non-empty static row or we wouldn't be here, but since 
                if (!staticRow.isEmpty() && !usesSecondaryIndexing && hasNoClusteringColumnsRestriction())
                {
                    result.newRow();
                    for (ColumnDefinition def : selection.getColumns())
                    {
                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                                result.add(keyComponents[def.position()]);
                                break;
                            case STATIC:
                                addValue(result, def, staticRow, options);
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
                for (ColumnDefinition def : selection.getColumns())
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                            result.add(keyComponents[def.position()]);
                            break;
                        case CLUSTERING_COLUMN:
                            result.add(row.clustering().get(def.position()));
                            break;
                        case COMPACT_VALUE:
                        case REGULAR:
                            addValue(result, def, row, options);
                            break;
                        case STATIC:
                            addValue(result, def, staticRow, options);
                            break;
                    }
                }
            }
        }
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, Row row, QueryOptions options)
    {
        if (def.isComplex())
        {
            // Collections are the only complex types we have so far
            assert def.type.isCollection();
            Iterator<Cell> cells = row.getCells(def);
            if (cells == null)
                result.add((ByteBuffer)null);
            else
                result.add(((CollectionType)def.type).serializeForNativeProtocol(cells, options.getProtocolVersion()));
        }
        else
        {
            result.add(row.getCell(def));
        }
    }

    private boolean hasNoClusteringColumnsRestriction()
    {
        for (int i = 0; i < columnRestrictions.length; i++)
            if (columnRestrictions[i] != null)
                return false;
        return true;
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return keyIsInRelation && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        assert orderingIndexes != null;

        List<Integer> idToSort = new ArrayList<Integer>();
        List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

        for (ColumnIdentifier identifier : parameters.orderings.keySet())
        {
            ColumnDefinition orderingColumn = cfm.getColumnDefinition(identifier);
            idToSort.add(orderingIndexes.get(orderingColumn.name));
            sorters.add(orderingColumn.type);
        }

        Comparator<List<ByteBuffer>> comparator = idToSort.size() == 1
                                                ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                                                : new CompositeComparator(sorters, idToSort);
        Collections.sort(cqlRows.rows, comparator);
    }

    private static boolean isReversedType(ColumnDefinition def)
    {
        return def.type instanceof ReversedType;
    }

    private boolean columnFilterIsIdentity()
    {
        for (Restriction r : columnRestrictions)
        {
            if (r != null)
                return false;
        }
        return true;
    }

    private boolean hasClusteringColumnsRestriction()
    {
        for (int i = 0; i < columnRestrictions.length; i++)
            if (columnRestrictions[i] != null)
                return true;
        return false;
    }

    private void validateDistinctSelection()
    throws InvalidRequestException
    {
        // TODO: we need to hanle distinct. That possibly mean a special filter? Or a slice one with Slices.NONE (but the underlying
        // code should handle it). Also, we need the returned iterator to not be empty; should probably always have a non-empty static row
        // even if a fake one that is actualy empty but != Rows.EMPTY_STATIC_ROW
        throw new UnsupportedOperationException();
        //Collection<ColumnDefinition> requestedColumns = selection.getColumns();
        //for (ColumnDefinition def : requestedColumns)
        //    if (def.kind != ColumnDefinition.Kind.PARTITION_KEY && def.kind != ColumnDefinition.Kind.STATIC)
        //        throw new InvalidRequestException(String.format("SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)", def.name));

        //// If it's a key range, we require that all partition key columns are selected so we don't have to bother with post-query grouping.
        //if (!isKeyRange)
        //    return;

        //for (ColumnDefinition def : cfm.partitionKeyColumns())
        //    if (!requestedColumns.contains(def))
        //        throw new InvalidRequestException(String.format("SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name));
    }

    public static class RawStatement extends CFStatement
    {
        private final Parameters parameters;
        private final List<RawSelector> selectClause;
        private final List<Relation> whereClause;
        private final Term.Raw limit;

        public RawStatement(CFName cfName, Parameters parameters, List<RawSelector> selectClause, List<Relation> whereClause, Term.Raw limit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause == null ? Collections.<Relation>emptyList() : whereClause;
            this.limit = limit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            Selection selection = selectClause.isEmpty()
                                ? Selection.wildcard(cfm)
                                : Selection.fromSelectors(cfm, selectClause);

            SelectStatement stmt = new SelectStatement(cfm, boundNames.size(), parameters, selection, prepareLimit(boundNames));

            /*
             * WHERE clause. For a given entity, rules are:
             *   - EQ relation conflicts with anything else (including a 2nd EQ)
             *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
             *   - IN relation are restricted to row keys (for now) and conflicts with anything else
             *     (we could allow two IN for the same entity but that doesn't seem very useful)
             *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value in CQL so far)
             */
            boolean hasQueriableIndex = false;
            boolean hasQueriableClusteringColumnIndex = false;
            boolean hasSingleColumnRelations = false;
            boolean hasMultiColumnRelations = false;
            for (Relation relation : whereClause)
            {
                if (relation.isMultiColumn())
                {
                    MultiColumnRelation rel = (MultiColumnRelation) relation;
                    List<ColumnDefinition> names = new ArrayList<>(rel.getEntities().size());
                    for (ColumnIdentifier entity : rel.getEntities())
                    {
                        ColumnDefinition def = cfm.getColumnDefinition(entity);
                        boolean[] queriable = processRelationEntity(stmt, relation, entity, def);
                        hasQueriableIndex |= queriable[0];
                        hasQueriableClusteringColumnIndex |= queriable[1];
                        names.add(def);
                        hasMultiColumnRelations |= ColumnDefinition.Kind.CLUSTERING_COLUMN.equals(def.kind);
                    }
                    updateRestrictionsForRelation(stmt, names, rel, boundNames);
                }
                else
                {
                    SingleColumnRelation rel = (SingleColumnRelation) relation;
                    ColumnIdentifier entity = rel.getEntity();
                    ColumnDefinition def = cfm.getColumnDefinition(entity);
                    boolean[] queriable = processRelationEntity(stmt, relation, entity, def);
                    hasQueriableIndex |= queriable[0];
                    hasQueriableClusteringColumnIndex |= queriable[1];
                    hasSingleColumnRelations |= ColumnDefinition.Kind.CLUSTERING_COLUMN.equals(def.kind);
                    updateRestrictionsForRelation(stmt, def, rel, boundNames);
                }
            }
            if (hasSingleColumnRelations && hasMultiColumnRelations)
                throw new InvalidRequestException("Mixing single column relations and multi column relations on clustering columns is not allowed");

             // At this point, the select statement if fully constructed, but we still have a few things to validate
            processPartitionKeyRestrictions(stmt, hasQueriableIndex, cfm);

            // All (or none) of the partition key columns have been specified;
            // hence there is no need to turn these restrictions into index expressions.
            if (!stmt.usesSecondaryIndexing)
                stmt.restrictedColumns.removeAll(cfm.partitionKeyColumns());

            if (stmt.selectsOnlyStaticColumns && stmt.hasClusteringColumnsRestriction())
                throw new InvalidRequestException("Cannot restrict clustering columns when selecting only static columns");

            processColumnRestrictions(stmt, hasQueriableIndex, cfm);

            // Covers indexes on the first clustering column (among others).
            if (stmt.isKeyRange && hasQueriableClusteringColumnIndex)
                stmt.usesSecondaryIndexing = true;

            if (!stmt.usesSecondaryIndexing)
                stmt.restrictedColumns.removeAll(cfm.clusteringColumns());

            // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
            // there is restrictions not covered by the PK.
            if (!stmt.metadataRestrictions.isEmpty())
            {
                if (!hasQueriableIndex)
                    throw new InvalidRequestException("No indexed columns present in by-columns clause with Equal operator");
                stmt.usesSecondaryIndexing = true;
            }

            if (stmt.usesSecondaryIndexing)
                validateSecondaryIndexSelections(stmt);

            if (!stmt.parameters.orderings.isEmpty())
                processOrderingClause(stmt, cfm);

            checkNeedsFiltering(stmt);

            if (parameters.isDistinct)
                stmt.validateDistinctSelection();

            return new ParsedStatement.Prepared(stmt, boundNames);
        }

        /** Returns a pair of (hasQueriableIndex, hasQueriableClusteringColumnIndex) */
        private boolean[] processRelationEntity(SelectStatement stmt, Relation relation, ColumnIdentifier entity, ColumnDefinition def) throws InvalidRequestException
        {
            if (def == null)
                handleUnrecognizedEntity(entity, relation);

            stmt.restrictedColumns.add(def);
            if (def.isIndexed() && relation.operator().allowsIndexQuery())
                return new boolean[]{true, def.kind == ColumnDefinition.Kind.CLUSTERING_COLUMN};
            return new boolean[]{false, false};
        }

        /** Throws an InvalidRequestException for an unrecognized identifier in the WHERE clause */
        private void handleUnrecognizedEntity(ColumnIdentifier entity, Relation relation) throws InvalidRequestException
        {
            if (containsAlias(entity))
                throw new InvalidRequestException(String.format("Aliases aren't allowed in the where clause ('%s')", relation));
            else
                throw new InvalidRequestException(String.format("Undefined name %s in where clause ('%s')", entity, relation));
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace(), limitReceiver());
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private void updateRestrictionsForRelation(SelectStatement stmt, List<ColumnDefinition> defs, MultiColumnRelation relation, VariableSpecifications boundNames) throws InvalidRequestException
        {
            List<ColumnDefinition> restrictedColumns = new ArrayList<>();
            Set<ColumnDefinition> seen = new HashSet<>();

            int previousPosition = -1;
            for (ColumnDefinition def : defs)
            {
                // ensure multi-column restriction only applies to clustering columns
                if (def.kind != ColumnDefinition.Kind.CLUSTERING_COLUMN)
                    throw new InvalidRequestException(String.format("Multi-column relations can only be applied to clustering columns: %s", def));

                if (seen.contains(def))
                    throw new InvalidRequestException(String.format("Column \"%s\" appeared twice in a relation: %s", def, relation));
                seen.add(def);

                // check that no clustering columns were skipped
                if (def.position() != previousPosition + 1)
                {
                    if (previousPosition == -1)
                        throw new InvalidRequestException(String.format(
                                "Clustering columns may not be skipped in multi-column relations. " +
                                "They should appear in the PRIMARY KEY order. Got %s", relation));
                    else
                        throw new InvalidRequestException(String.format(
                                "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", relation));
                }
                previousPosition++;

                Restriction existing = getExistingRestriction(stmt, def);
                Relation.Type operator = relation.operator();
                if (existing != null)
                {
                    if (operator == Relation.Type.EQ || operator == Relation.Type.IN)
                        throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by more than one relation if it is in an %s relation", def, relation.operator()));
                    else if (!existing.isSlice())
                        throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by an equality relation and an inequality relation", def));
                }
                restrictedColumns.add(def);
            }

            switch (relation.operator())
            {
                case EQ:
                {
                    Term t = relation.getValue().prepare(keyspace(), defs);
                    t.collectMarkerSpecification(boundNames);
                    Restriction restriction = new MultiColumnRestriction.EQ(t, false);
                    for (ColumnDefinition def : restrictedColumns)
                        stmt.columnRestrictions[def.position()] = restriction;
                    break;
                }
                case IN:
                {
                    Restriction restriction;
                    List<? extends Term.MultiColumnRaw> inValues = relation.getInValues();
                    if (inValues != null)
                    {
                        // we have something like "(a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) or
                        // "(a, b, c) IN (?, ?, ?)
                        List<Term> terms = new ArrayList<>(inValues.size());
                        for (Term.MultiColumnRaw tuple : inValues)
                        {
                            Term t = tuple.prepare(keyspace(), defs);
                            t.collectMarkerSpecification(boundNames);
                            terms.add(t);
                        }
                         restriction = new MultiColumnRestriction.InWithValues(terms);
                    }
                    else
                    {
                        Tuples.INRaw rawMarker = relation.getInMarker();
                        AbstractMarker t = rawMarker.prepare(keyspace(), defs);
                        t.collectMarkerSpecification(boundNames);
                        restriction = new MultiColumnRestriction.InWithMarker(t);
                    }
                    for (ColumnDefinition def : restrictedColumns)
                        stmt.columnRestrictions[def.position()] = restriction;

                    break;
                }
                case LT:
                case LTE:
                case GT:
                case GTE:
                {
                    Term t = relation.getValue().prepare(keyspace(), defs);
                    t.collectMarkerSpecification(boundNames);
                    for (ColumnDefinition def : defs)
                    {
                        Restriction.Slice restriction = (Restriction.Slice)getExistingRestriction(stmt, def);
                        if (restriction == null)
                            restriction = new MultiColumnRestriction.Slice(false);
                        else if (!restriction.isMultiColumn())
                            throw new InvalidRequestException(String.format("Column \"%s\" cannot have both tuple-notation inequalities and single-column inequalities: %s", def.name, relation));
                        restriction.setBound(def.name, relation.operator(), t);
                        stmt.columnRestrictions[def.position()] = restriction;
                    }
                    break;
                }
                case NEQ:
                    throw new InvalidRequestException(String.format("Unsupported \"!=\" relation: %s", relation));
            }
        }

        private Restriction getExistingRestriction(SelectStatement stmt, ColumnDefinition def)
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    return stmt.keyRestrictions[def.position()];
                case CLUSTERING_COLUMN:
                    return stmt.columnRestrictions[def.position()];
                case REGULAR:
                case STATIC:
                    return stmt.metadataRestrictions.get(def.name);
                default:
                    throw new AssertionError();
            }
        }

        private void updateRestrictionsForRelation(SelectStatement stmt, ColumnDefinition def, SingleColumnRelation relation, VariableSpecifications names) throws InvalidRequestException
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    stmt.keyRestrictions[def.position()] = updateSingleColumnRestriction(def, stmt.keyRestrictions[def.position()], relation, names);
                    break;
                case CLUSTERING_COLUMN:
                    stmt.columnRestrictions[def.position()] = updateSingleColumnRestriction(def, stmt.columnRestrictions[def.position()], relation, names);
                    break;
                case COMPACT_VALUE:
                    throw new InvalidRequestException(String.format("Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", def.name));
                case REGULAR:
                case STATIC:
                    // We only all IN on the row key and last clustering key so far, never on non-PK columns, and this even if there's an index
                    Restriction r = updateSingleColumnRestriction(def, stmt.metadataRestrictions.get(def.name), relation, names);
                    if (r.isIN() && !((Restriction.IN)r).canHaveOnlyOneValue())
                        // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that slide.
                        throw new InvalidRequestException(String.format("IN predicates on non-primary-key columns (%s) is not yet supported", def.name));
                    stmt.metadataRestrictions.put(def.name, r);
                    break;
            }
        }

        Restriction updateSingleColumnRestriction(ColumnDefinition def, Restriction existingRestriction, SingleColumnRelation newRel, VariableSpecifications boundNames) throws InvalidRequestException
        {
            ColumnSpecification receiver = def;
            if (newRel.onToken)
            {
                if (def.kind != ColumnDefinition.Kind.PARTITION_KEY)
                    throw new InvalidRequestException(String.format("The token() function is only supported on the partition key, found on %s", def.name));

                receiver = new ColumnSpecification(def.ksName,
                                                   def.cfName,
                                                   new ColumnIdentifier("partition key token", true),
                                                   StorageService.getPartitioner().getTokenValidator());
            }

            // We don't support relations against entire collections, like "numbers = {1, 2, 3}"
            if (receiver.type.isCollection() && !(newRel.operator().equals(Relation.Type.CONTAINS_KEY) || newRel.operator() == Relation.Type.CONTAINS))
            {
                throw new InvalidRequestException(String.format("Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                                                                def.name, receiver.type.asCQL3Type(), newRel.operator()));
            }

            switch (newRel.operator())
            {
                case EQ:
                {
                    if (existingRestriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes an Equal", def.name));
                    Term t = newRel.getValue().prepare(keyspace(), receiver);
                    t.collectMarkerSpecification(boundNames);
                    existingRestriction = new SingleColumnRestriction.EQ(t, newRel.onToken);
                }
                break;
                case IN:
                    if (existingRestriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes a IN", def.name));

                    if (newRel.getInValues() == null)
                    {
                        // Means we have a "SELECT ... IN ?"
                        assert newRel.getValue() != null;
                        Term t = newRel.getValue().prepare(keyspace(), receiver);
                        t.collectMarkerSpecification(boundNames);
                        existingRestriction = new SingleColumnRestriction.InWithMarker((Lists.Marker)t);
                    }
                    else
                    {
                        List<Term> inValues = new ArrayList<>(newRel.getInValues().size());
                        for (Term.Raw raw : newRel.getInValues())
                        {
                            Term t = raw.prepare(keyspace(), receiver);
                            t.collectMarkerSpecification(boundNames);
                            inValues.add(t);
                        }
                        existingRestriction = new SingleColumnRestriction.InWithValues(inValues);
                    }
                    break;
                case NEQ:
                    throw new InvalidRequestException(String.format("Unsupported \"!=\" relation on column \"%s\"", def.name));
                case GT:
                case GTE:
                case LT:
                case LTE:
                    {
                        if (existingRestriction == null)
                            existingRestriction = new SingleColumnRestriction.Slice(newRel.onToken);
                        else if (!existingRestriction.isSlice())
                            throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by both an equality and an inequality relation", def.name));
                        else if (existingRestriction.isMultiColumn())
                            throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by both a tuple notation inequality and a single column inequality (%s)", def.name, newRel));
                        else if (existingRestriction.isOnToken() != newRel.onToken)
                            // For partition keys, we shouldn't have slice restrictions without token(). And while this is rejected later by
                            // processPartitionKeysRestrictions, we shouldn't update the existing restriction by the new one if the old one was using token()
                            // and the new one isn't since that would bypass that later test.
                            throw new InvalidRequestException("Only EQ and IN relation are supported on the partition key (unless you use the token() function)");

                        Term t = newRel.getValue().prepare(keyspace(), receiver);
                        t.collectMarkerSpecification(boundNames);
                        ((SingleColumnRestriction.Slice)existingRestriction).setBound(def.name, newRel.operator(), t);
                    }
                    break;
                case CONTAINS_KEY:
                    if (!(receiver.type instanceof MapType))
                        throw new InvalidRequestException(String.format("Cannot use CONTAINS_KEY on non-map column %s", def.name));
                    // Fallthrough on purpose
                case CONTAINS:
                {
                    if (!receiver.type.isCollection())
                        throw new InvalidRequestException(String.format("Cannot use %s relation on non collection column %s", newRel.operator(), def.name));

                    if (existingRestriction == null)
                        existingRestriction = new SingleColumnRestriction.Contains();
                    else if (!existingRestriction.isContains())
                        throw new InvalidRequestException(String.format("Collection column %s can only be restricted by CONTAINS or CONTAINS KEY", def.name));
                    boolean isKey = newRel.operator() == Relation.Type.CONTAINS_KEY;
                    receiver = makeCollectionReceiver(receiver, isKey);
                    Term t = newRel.getValue().prepare(keyspace(), receiver);
                    t.collectMarkerSpecification(boundNames);
                    ((SingleColumnRestriction.Contains)existingRestriction).add(t, isKey);
                    break;
                }
            }
            return existingRestriction;
        }

        private void processPartitionKeyRestrictions(SelectStatement stmt, boolean hasQueriableIndex, CFMetaData cfm) throws InvalidRequestException
        {
            // If there is a queriable index, no special condition are required on the other restrictions.
            // But we still need to know 2 things:
            //   - If we don't have a queriable index, is the query ok
            //   - Is it queriable without 2ndary index, which is always more efficient
            // If a component of the partition key is restricted by a relation, all preceding
            // components must have a EQ. Only the last partition key component can be in IN relation.
            boolean canRestrictFurtherComponents = true;
            ColumnDefinition previous = null;
            stmt.keyIsInRelation = false;
            Iterator<ColumnDefinition> iter = cfm.partitionKeyColumns().iterator();
            for (int i = 0; i < stmt.keyRestrictions.length; i++)
            {
                ColumnDefinition cdef = iter.next();
                Restriction restriction = stmt.keyRestrictions[i];

                if (restriction == null)
                {
                    if (stmt.onToken)
                        throw new InvalidRequestException("The token() function must be applied to all partition key components or none of them");

                    // The only time not restricting a key part is allowed is if none are restricted or an index is used.
                    if (i > 0 && stmt.keyRestrictions[i - 1] != null)
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true;
                            stmt.isKeyRange = true;
                            break;
                        }
                        throw new InvalidRequestException(String.format("Partition key part %s must be restricted since preceding part is", cdef.name));
                    }

                    stmt.isKeyRange = true;
                    canRestrictFurtherComponents = false;
                }
                else if (!canRestrictFurtherComponents)
                {
                    if (hasQueriableIndex)
                    {
                        stmt.usesSecondaryIndexing = true;
                        break;
                    }
                    throw new InvalidRequestException(String.format(
                            "Partitioning column \"%s\" cannot be restricted because the preceding column (\"%s\") is " +
                            "either not restricted or is restricted by a non-EQ relation", cdef.name, previous));
                }
                else if (restriction.isOnToken())
                {
                    // If this is a query on tokens, it's necessarily a range query (there can be more than one key per token).
                    stmt.isKeyRange = true;
                    stmt.onToken = true;
                }
                else if (stmt.onToken)
                {
                    throw new InvalidRequestException(String.format("The token() function must be applied to all partition key components or none of them"));
                }
                else if (!restriction.isSlice())
                {
                    if (restriction.isIN())
                    {
                        // We only support IN for the last name so far
                        if (i != stmt.keyRestrictions.length - 1)
                            throw new InvalidRequestException(String.format("Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)", cdef.name));
                        stmt.keyIsInRelation = true;
                    }
                }
                else
                {
                    // Non EQ relation is not supported without token(), even if we have a 2ndary index (since even those are ordered by partitioner).
                    // Note: In theory we could allow it for 2ndary index queries with ALLOW FILTERING, but that would probably require some special casing
                    // Note bis: This is also why we don't bother handling the 'tuple' notation of #4851 for keys. If we lift the limitation for 2ndary
                    // index with filtering, we'll need to handle it though.
                    throw new InvalidRequestException("Only EQ and IN relation are supported on the partition key (unless you use the token() function)");
                }
                previous = cdef;
            }

            if (stmt.onToken)
                checkTokenFunctionArgumentsOrder(cfm);
        }

        /**
         * Checks that the column identifiers used as argument for the token function have been specified in the
         * partition key order.
         * @param cfm the Column Family MetaData
         * @throws InvalidRequestException if the arguments have not been provided in the proper order.
         */
        private void checkTokenFunctionArgumentsOrder(CFMetaData cfm) throws InvalidRequestException
        {
            Iterator<ColumnDefinition> iter = Iterators.cycle(cfm.partitionKeyColumns());
            for (Relation relation : whereClause)
            {
                SingleColumnRelation singleColumnRelation = (SingleColumnRelation) relation;
                if (singleColumnRelation.onToken && !cfm.getColumnDefinition(singleColumnRelation.getEntity()).equals(iter.next()))
                    throw new InvalidRequestException(String.format("The token function arguments must be in the partition key order: %s",
                                                                    Joiner.on(',').join(cfm.partitionKeyColumns())));
            }
        }

        private void processColumnRestrictions(SelectStatement stmt, boolean hasQueriableIndex, CFMetaData cfm) throws InvalidRequestException
        {
            // If a clustering key column is restricted by a non-EQ relation, all preceding
            // columns must have a EQ, and all following must have no restriction. Unless
            // the column is indexed that is.
            boolean canRestrictFurtherComponents = true;
            ColumnDefinition previous = null;
            boolean previousIsSlice = false;
            Iterator<ColumnDefinition> iter = cfm.clusteringColumns().iterator();
            for (int i = 0; i < stmt.columnRestrictions.length; i++)
            {
                ColumnDefinition cdef = iter.next();
                Restriction restriction = stmt.columnRestrictions[i];

                if (restriction == null)
                {
                    canRestrictFurtherComponents = false;
                    previousIsSlice = false;
                }
                else if (!canRestrictFurtherComponents)
                {
                    // We're here if the previous clustering column was either not restricted or was a slice.
                    // We can't restrict the current column unless:
                    //   1) we're in the special case of the 'tuple' notation from #4851 which we expand as multiple
                    //      consecutive slices: in which case we're good with this restriction and we continue
                    //   2) we have a 2ndary index, in which case we have to use it but can skip more validation
                    if (!(previousIsSlice && restriction.isSlice() && restriction.isMultiColumn()))
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true; // handle gaps and non-keyrange cases.
                            break;
                        }
                        throw new InvalidRequestException(String.format(
                                "PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is either not restricted or by a non-EQ relation)", cdef.name, previous));
                    }
                }
                else if (restriction.isSlice())
                {
                    canRestrictFurtherComponents = false;
                    previousIsSlice = true;
                }
                else if (restriction.isIN())
                {
                    if (stmt.selectACollection())
                        throw new InvalidRequestException(String.format("Cannot restrict column \"%s\" by IN relation as a collection is selected by the query", cdef.name));
                }

                previous = cdef;
            }
        }

        private void validateSecondaryIndexSelections(SelectStatement stmt) throws InvalidRequestException
        {
            if (stmt.keyIsInRelation)
                throw new InvalidRequestException("Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
            // When the user only select static columns, the intent is that we don't query the whole partition but just
            // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on static columns
            // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
            if (stmt.selectsOnlyStaticColumns)
                throw new InvalidRequestException("Queries using 2ndary indexes don't support selecting only static columns");            
        }

        private void verifyOrderingIsAllowed(SelectStatement stmt) throws InvalidRequestException
        {
            if (stmt.usesSecondaryIndexing)
                throw new InvalidRequestException("ORDER BY with 2ndary indexes is not supported.");

            if (stmt.isKeyRange)
                throw new InvalidRequestException("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private void handleUnrecognizedOrderingColumn(ColumnIdentifier column) throws InvalidRequestException
        {
            if (containsAlias(column))
                throw new InvalidRequestException(String.format("Aliases are not allowed in order by clause ('%s')", column));
            else
                throw new InvalidRequestException(String.format("Order by on unknown column %s", column));
        }

        private void processOrderingClause(SelectStatement stmt, CFMetaData cfm) throws InvalidRequestException
        {
            verifyOrderingIsAllowed(stmt);

            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting, even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            if (stmt.keyIsInRelation)
            {
                stmt.orderingIndexes = new HashMap<>();
                for (ColumnIdentifier column : stmt.parameters.orderings.keySet())
                {
                    final ColumnDefinition def = cfm.getColumnDefinition(column);
                    if (def == null)
                        handleUnrecognizedOrderingColumn(column);

                    int index = indexOf(def, stmt.selection);
                    if (index < 0)
                        index = stmt.selection.addColumnForOrdering(def);
                    stmt.orderingIndexes.put(def.name, index);
                }
            }
            stmt.isReversed = isReversed(stmt, cfm);
        }

        private boolean isReversed(SelectStatement stmt, CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnIdentifier, Boolean> entry : stmt.parameters.orderings.entrySet())
            {
                ColumnIdentifier column = entry.getKey();
                boolean reversed = entry.getValue();

                ColumnDefinition def = cfm.getColumnDefinition(column);
                if (def == null)
                    handleUnrecognizedOrderingColumn(column);

                if (def.kind != ColumnDefinition.Kind.CLUSTERING_COLUMN)
                    throw new InvalidRequestException(String.format("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", column));

                if (i++ != def.position())
                    throw new InvalidRequestException(String.format("Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY"));

                reversedMap[def.position()] = (reversed != isReversedType(def));
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
                if (!isReversed.equals(b))
                    throw new InvalidRequestException(String.format("Unsupported order by relation"));
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(SelectStatement stmt) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (stmt.isKeyRange || stmt.usesSecondaryIndexing))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the column filter is not the identity
                if (stmt.restrictedColumns.size() > 1 || (stmt.restrictedColumns.isEmpty() && !stmt.columnFilterIsIdentity()))
                    throw new InvalidRequestException("Cannot execute this query as it might involve data filtering and " +
                                                      "thus may have unpredictable performance. If you want to execute " +
                                                      "this query despite the performance unpredictability, use ALLOW FILTERING");
            }
        }

        private int indexOf(ColumnDefinition def, Selection selection)
        {
            return indexOf(def, selection.getColumns().iterator());
        }

        private int indexOf(final ColumnDefinition def, Iterator<ColumnDefinition> defs)
        {
            return Iterators.indexOf(defs, new Predicate<ColumnDefinition>()
                                           {
                                               public boolean apply(ColumnDefinition n)
                                               {
                                                   return def.name.equals(n.name);
                                               }
                                           });
        }

        private boolean containsAlias(final ColumnIdentifier name)
        {
            return Iterables.any(selectClause, new Predicate<RawSelector>()
                                               {
                                                   public boolean apply(RawSelector raw)
                                                   {
                                                       return name.equals(raw.alias);
                                                   }
                                               });
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private static ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
        {
            assert collection.type.isCollection();
            switch (((CollectionType)collection.type).kind)
            {
                case LIST:
                    assert !isKey;
                    return Lists.valueSpecOf(collection);
                case SET:
                    assert !isKey;
                    return Sets.valueSpecOf(collection);
                case MAP:
                    return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            }
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                          .add("name", cfName)
                          .add("selectClause", selectClause)
                          .add("whereClause", whereClause)
                          .add("isDistinct", parameters.isDistinct)
                          .toString();
        }
    }

    public static class Parameters
    {
        private final Map<ColumnIdentifier, Boolean> orderings;
        private final boolean isDistinct;
        private final boolean allowFiltering;

        public Parameters(Map<ColumnIdentifier, Boolean> orderings,
                          boolean isDistinct,
                          boolean allowFiltering)
        {
            this.orderings = orderings;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator implements Comparator<List<ByteBuffer>>
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
            return comparator.compare(a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator implements Comparator<List<ByteBuffer>>
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

                ByteBuffer aValue = a.get(columnPos);
                ByteBuffer bValue = b.get(columnPos);

                int comparison = type.compare(aValue, bValue);

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
