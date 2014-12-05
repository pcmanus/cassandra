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
package org.apache.cassandra.db.filters;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;

public class ColumnFilter implements Iterable<ColumnFilter.Expression>
{
    public enum Operator
    {
        EQ, GTE, GT, LTE, LT, CONTAINS, CONTAINS_KEY;

        public static Operator findByOrdinal(int ordinal)
        {
            switch (ordinal) {
                case 0:
                    return EQ;
                case 1:
                    return GTE;
                case 2:
                    return GT;
                case 3:
                    return LTE;
                case 4:
                    return LT;
                case 5:
                    return CONTAINS;
                case 6:
                    return CONTAINS_KEY;
                default:
                    throw new AssertionError();
            }
        }

        public boolean allowsIndexQuery()
        {
            switch (this)
            {
                case EQ:
                case CONTAINS:
                case CONTAINS_KEY:
                    return true;
                default:
                    return false;
            }
        }
    }

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
        expressions.add(new Expression(def, op, value));
    }

    public PartitionIterator filter(PartitionIterator iter)
    {
        if (expressions.isEmpty())
            return iter;

        // TODO
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return expressions.isEmpty();
    }

    public Iterator<Expression> iterator()
    {
        return expressions.iterator();
    }

    public static class Expression
    {
        private final ColumnDefinition column;
        private final Operator operator;
        private final ByteBuffer value;

        private Expression(ColumnDefinition column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator();
        }

        public ByteBuffer value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return String.format("%s %s %s", column.name, operator, column.type.getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Expression))
                return false;

            Expression that = (Expression)o;

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

    @Override
    public String toString()
    {
        if (expressions.isEmpty())
            return "NONE";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < expressions.size(); i++)
        {
            if (i > 0)
                sb.append(" AND ");
            sb.append(expressions.get(i));
        }
        return sb.toString();
    }
}
