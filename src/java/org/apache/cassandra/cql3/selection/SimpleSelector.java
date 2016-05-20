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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.Selection.ResultSetBuilder;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public final class SimpleSelector extends Selector
{
   public final ColumnDefinition column;
    private final int idx;
    private ByteBuffer current;
    private boolean isSet;

    private SimpleSelector(ColumnDefinition column, int idx)
    {
        this.column = column;
        this.idx = idx;
    }

    public static Factory newFactory(final ColumnDefinition def, final int idx)
    {
        return new Factory(def, idx);
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        builder.add(column);
    }

    @Override
    public void addInput(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        if (!isSet)
        {
            isSet = true;
            current = rs.current.get(idx);
        }
    }

    @Override
    public ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException
    {
        return current;
    }

    @Override
    public void reset()
    {
        isSet = false;
        current = null;
    }

    @Override
    public AbstractType<?> getType()
    {
        return column.type;
    }

    @Override
    public String toString()
    {
        return column.name.toString();
    }

    public static class Factory extends Selector.Factory
    {
        private final ColumnDefinition def;
        private final int idx;

        private Factory(ColumnDefinition def, int idx)
        {
            this.def = def;
            this.idx = idx;
        }

        protected String getColumnName()
        {
            return def.name.toString();
        }

        protected AbstractType<?> getReturnType()
        {
            return def.type;
        }

        protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
        {
            mapping.addMapping(resultColumn, def);
        }

        public Selector newInstance(QueryOptions options)
        {
            return new SimpleSelector(def, idx);
        }

        @Override
        public boolean isSimpleSelectorFactory(int index)
        {
            return index == idx;
        }

        @Override
        public boolean isSimpleSelectorFactory()
        {
            return true;
        }

        public ColumnDefinition getColumn()
        {
            return def;
        }

        public boolean isTerminal()
        {
            return true;
        }

        public void addFetchedColumns(ColumnFilter.Builder builder)
        {
            builder.add(def);
        }
    }
}
