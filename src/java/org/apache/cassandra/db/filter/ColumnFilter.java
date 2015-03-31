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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnFilter implements Iterable<ColumnFilter.Expression>
{
    public static final Serializer serializer = new Serializer();
    public static final ColumnFilter NONE = new ColumnFilter(Collections.<Expression>emptyList());

    private final List<Expression> expressions;

    private ColumnFilter(List<Expression> expressions)
    {
        this.expressions = expressions;
    }

    public ColumnFilter(int capacity)
    {
        this(new ArrayList<Expression>(capacity));
    }

    public ColumnFilter()
    {
        this(new ArrayList<Expression>());
    }

    public void add(ColumnDefinition def, Operator op, ByteBuffer value)
    {
        expressions.add(new SimpleExpression(def, op, value));
    }

    public void addMapEquality(ColumnDefinition def, ByteBuffer key, Operator op, ByteBuffer value)
    {
        expressions.add(new MapEqualityExpression(def, key, op, value));
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @return the filtered iterator.
     */
    public PartitionIterator filter(PartitionIterator iter)
    {
        if (expressions.isEmpty())
            return iter;

        return new WrappingPartitionIterator(iter)
        {
            @Override
            public AtomIterator computeNext(final AtomIterator iter)
            {
                return new RowFilteringAtomIterator(iter, new TombstoneFilteringRow())
                {
                    protected boolean includeRow(Row row)
                    {
                        return row.isStatic() || ColumnFilter.this.isSatisfiedBy(iter.partitionKey(), row);
                    }
                };
            }
        };
    }

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression.
     */
    public ColumnFilter without(Expression expression)
    {
        assert expressions.contains(expression);
        if (expressions.size() == 1)
            return ColumnFilter.NONE;

        List<Expression> newExpressions = new ArrayList<>(expressions.size() - 1);
        for (Expression e : expressions)
            if (!e.equals(expression))
                newExpressions.add(e);
        return new ColumnFilter(newExpressions);
    }

    /**
     * Returns whether the provided row (with it's partition key) satisfies
     * this column filter or not (that is, if it satisfies all of its expressions).
     */
    public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
    {
        for (Expression e : expressions)
            if (!e.isSatisfiedBy(partitionKey, row))
                return false;

        return true;
    }

    public boolean isEmpty()
    {
        return expressions.isEmpty();
    }

    public Iterator<Expression> iterator()
    {
        return expressions.iterator();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < expressions.size(); i++)
        {
            if (i > 0)
                sb.append(" AND ");
            sb.append(expressions.get(i));
        }
        return sb.toString();
    }

    public static abstract class Expression
    {
        private static final Serializer serializer = new Serializer();

        // Note: the order of this enum matter, it's used for serialization
        protected enum Kind { SIMPLE, MAP_EQUALITY }

        private final Kind kind;
        protected final ColumnDefinition column;
        protected final Operator operator;

        protected Expression(Kind kind, ColumnDefinition column, Operator operator)
        {
            this.kind = kind;
            this.column = column;
            this.operator = operator;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContains()
        {
            return Operator.CONTAINS == operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContainsKey()
        {
            return Operator.CONTAINS_KEY == operator;
        }

        /**
         * If this expression is used to query an index, the value to use as
         * partition key for that index query.
         */
        public abstract ByteBuffer getIndexValue();

        public abstract void validateForIndexing() throws InvalidRequestException;

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(DecoratedKey partitionKey, Row row);

        protected ByteBuffer getValue(DecoratedKey partitionKey, Row row)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return column.isOnAllComponents()
                         ? partitionKey.getKey()
                         : CompositeType.extractComponent(partitionKey.getKey(), column.position());
                case CLUSTERING_COLUMN:
                    return row.clustering().get(column.position());
                default:
                    Cell cell = row.getCell(column);
                    return cell == null ? null : cell.value();
            }
        }

        private static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                if (version >= MessagingService.VERSION_30)
                    out.writeByte(expression.kind.ordinal());

                switch (expression.kind)
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(((SimpleExpression)expression).value, out);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        if (version < MessagingService.VERSION_30)
                        {
                            ByteBufferUtil.writeWithShortLength(mexpr.getIndexValue(), out);
                        }
                        else
                        {
                            ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                            ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        }
                        break;
                }
            }

            public Expression deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                Operator operator = Operator.readFrom(in);

                Kind kind = version >= MessagingService.VERSION_30
                          ? Kind.values()[in.readByte()]
                          : (column.type instanceof MapType && operator == Operator.EQ ? Kind.MAP_EQUALITY : Kind.SIMPLE);

                switch (kind)
                {
                    case SIMPLE:
                        return new SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                    case MAP_EQUALITY:
                        ByteBuffer key, value;
                        if (version < MessagingService.VERSION_30)
                        {
                            ByteBuffer composite = ByteBufferUtil.readWithShortLength(in);
                            key = CompositeType.extractComponent(composite, 0);
                            value = CompositeType.extractComponent(composite, 0);
                        }
                        else
                        {
                            key = ByteBufferUtil.readWithShortLength(in);
                            value = ByteBufferUtil.readWithShortLength(in);
                        }
                        return new MapEqualityExpression(column, key, operator, value);
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                TypeSizes sizes = TypeSizes.NATIVE;
                long size = ByteBufferUtil.serializedSizeWithShortLength(expression.column.name.bytes, sizes)
                          + expression.operator.serializedSize();

                switch (expression.kind)
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength(((SimpleExpression)expression).value, sizes);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        if (version < MessagingService.VERSION_30)
                            size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.getIndexValue(), sizes);
                        else
                            size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key, sizes)
                                  + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value, sizes);
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    private static class SimpleExpression extends Expression
    {
        private final ByteBuffer value;

        public SimpleExpression(ColumnDefinition column, Operator operator, ByteBuffer value)
        {
            super(Kind.SIMPLE, column, operator);
            this.value = value;
        }

        public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for ColumnFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            switch (operator)
            {
                case EQ:
                case LT:
                case LTE:
                case GTE:
                case GT:
                case NEQ:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                    }
                case CONTAINS:
                    assert column.type.isCollection();
                    CollectionType type = (CollectionType)column.type;
                    if (column.isComplex())
                    {
                        Iterator<Cell> iter = row.getCells(column);
                        while (iter.hasNext())
                        {
                            Cell cell = iter.next();
                            if (type.kind == CollectionType.Kind.SET)
                            {
                                if (type.nameComparator().compare(cell.path().get(0), value) == 0)
                                    return true;
                            }
                            else
                            {
                                if (type.valueComparator().compare(cell.value(), value) == 0)
                                    return true;
                            }
                        }
                        return false;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        if (foundValue == null)
                            return false;

                        switch (type.kind)
                        {
                            case LIST:
                                ListType<?> listType = (ListType)type;
                                return listType.compose(foundValue).contains(listType.getElementsType().compose(value));
                            case SET:
                                SetType<?> setType = (SetType)type;
                                return setType.compose(foundValue).contains(setType.getElementsType().compose(value));
                            case MAP:
                                MapType<?,?> mapType = (MapType)type;
                                return mapType.compose(foundValue).containsValue(mapType.getValuesType().compose(value));
                        }
                        throw new AssertionError();
                    }
                case CONTAINS_KEY:
                    assert column.type.isCollection() && column.type instanceof MapType;
                    MapType<?, ?> mapType = (MapType)column.type;
                    if (column.isComplex())
                    {
                         return row.getCell(column, CellPath.create(value)) != null;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, value, mapType.getKeysType()) != null;
                    }

                case IN:
                    // It wouldn't be terribly hard to support this (though doing so would imply supporting
                    // IN for 2ndary index) but currently we don't.
                    throw new AssertionError();
            }
            throw new AssertionError();
        }

        public ByteBuffer getIndexValue()
        {
            return value;
        }

        public void validateForIndexing() throws InvalidRequestException
        {
            if (value == null)
                throw new InvalidRequestException("Unsupported null value for indexed column " + column.name);
            if (value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException("Index expression values may not be larger than 64K");
        }

        @Override
        public String toString()
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType ct = (CollectionType)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType)type).nameComparator();
                    break;
                case IN:
                    type = ListType.getInstance(type, false);
                    break;
            }
            return String.format("%s %s %s", column.name, operator, type.getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof SimpleExpression))
                return false;

            SimpleExpression that = (SimpleExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }
    }

    /**
     * An expression of the form 'column' ['key'] = 'value' (which is only
     * supported when 'column' is a map).
     */
    private static class MapEqualityExpression extends Expression
    {
        private final ByteBuffer key;
        private final ByteBuffer value;

        public MapEqualityExpression(ColumnDefinition column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(Kind.MAP_EQUALITY, column, operator);
            assert column.type instanceof MapType && operator == Operator.EQ;
            this.key = key;
            this.value = value;
        }

        public void validateForIndexing() throws InvalidRequestException
        {
            if (key == null)
                throw new InvalidRequestException("Unsupported null key for indexed column " + column.name);
            if (value == null)
                throw new InvalidRequestException("Unsupported null value for indexed column " + column.name);
            if (getIndexValue().remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException("Index expression values may not be larger than 64K");
        }

        public ByteBuffer getIndexValue()
        {
            return CompositeType.build(key, value);
        }

        public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            assert key != null;
            // We support null conditions for LWT (in ColumnCondition) but not for ColumnFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            MapType mt = (MapType)column.type;
            if (column.isComplex())
            {
                Cell cell = row.getCell(column, CellPath.create(key));
                return cell != null && mt.valueComparator().compare(cell.value(), value) == 0;
            }
            else
            {
                ByteBuffer serializedMap = getValue(partitionKey, row);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                return foundValue != null && mt.valueComparator().compare(foundValue, value) == 0;
            }
        }

        @Override
        public String toString()
        {
            MapType mt = (MapType)column.type;
            return String.format("%s[%s] = %s", column.name, mt.nameComparator().getString(key), mt.valueComparator().getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof MapEqualityExpression))
                return false;

            MapEqualityExpression that = (MapEqualityExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.key, that.key)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, key, value);
        }
    }

    public static class Serializer
    {
        public void serialize(ColumnFilter filter, DataOutputPlus out, int version) throws IOException
        {
            out.writeShort(filter.expressions.size());
            for (Expression expr : filter.expressions)
                Expression.serializer.serialize(expr, out, version);
        }

        public ColumnFilter deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
        {
            int size = in.readUnsignedShort();
            List<Expression> expressions = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                expressions.add(Expression.serializer.deserialize(in, version, metadata));
            return new ColumnFilter(expressions);
        }

        public long serializedSize(ColumnFilter filter, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            long size = sizes.sizeof((short)filter.expressions.size());
            for (Expression expr : filter.expressions)
                size += Expression.serializer.serializedSize(expr, version);
            return size;
        }
    }
}
