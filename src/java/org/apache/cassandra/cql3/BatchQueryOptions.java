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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.pager.PagingState;

public abstract class BatchQueryOptions
{
    public static BatchQueryOptions DEFAULT = withoutPerStatementVariables(QueryOptions.DEFAULT);

    protected final QueryOptions wrapped;

    protected BatchQueryOptions(QueryOptions wrapped)
    {
        this.wrapped = wrapped;
    }

    public static BatchQueryOptions withoutPerStatementVariables(QueryOptions options)
    {
        return new WithoutPerStatementVariables(options);
    }

    public static BatchQueryOptions withPerStatementVariables(QueryOptions options, List<List<ByteBuffer>> variables)
    {
        return new WithPerStatementVariables(options, variables);
    }

    public abstract QueryOptions forStatement(int i);

    public ConsistencyLevel getConsistency()
    {
        return wrapped.getConsistency();
    }

    public ConsistencyLevel getSerialConsistency()
    {
        return wrapped.getSerialConsistency();
    }

    private static class WithoutPerStatementVariables extends BatchQueryOptions
    {
        private WithoutPerStatementVariables(QueryOptions wrapped)
        {
            super(wrapped);
        }

        public QueryOptions forStatement(int i)
        {
            return wrapped;
        }
    }

    private static class WithPerStatementVariables extends BatchQueryOptions
    {
        private final List<QueryOptionsWrapper> perStatementOptions;

        private WithPerStatementVariables(QueryOptions wrapped, List<List<ByteBuffer>> variables)
        {
            super(wrapped);
            this.perStatementOptions = new ArrayList<>(variables.size());
            for (List<ByteBuffer> vars : variables)
                perStatementOptions.add(new QueryOptionsWrapper(wrapped, vars));
        }

        public QueryOptions forStatement(int i)
        {
            return perStatementOptions.get(i);
        }

        private static class QueryOptionsWrapper extends QueryOptions
        {
            private final QueryOptions wrapped;
            private final List<ByteBuffer> values;

            private QueryOptionsWrapper(QueryOptions wrapped, List<ByteBuffer> values)
            {
                this.wrapped = wrapped;
                this.values = values;
            }

            public ConsistencyLevel getConsistency()
            {
                return wrapped.getConsistency();
            }

            public List<ByteBuffer> getValues()
            {
                return values;
            }

            public boolean skipMetadata()
            {
                return wrapped.skipMetadata();
            }

            public int getPageSize()
            {
                return wrapped.getPageSize();
            }

            public PagingState getPagingState()
            {
                return wrapped.getPagingState();
            }

            public ConsistencyLevel getSerialConsistency()
            {
                return wrapped.getSerialConsistency();
            }

            public int getProtocolVersion()
            {
                return wrapped.getProtocolVersion();
            }

            SpecificOptions getSpecificOptions()
            {
                return wrapped.getSpecificOptions();
            }

            public QueryOptions withProtocolVersion(int version)
            {
                return new DefaultQueryOptions(getConsistency(), getValues(), skipMetadata(),  getSpecificOptions(), version);
            }
        }
    }
}
