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

import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * Columns (or a subset of the columns) that a partition contains.
 * This mainly groups both static and regular columns for convenience.
 */
public class PartitionColumns
{
    public final Columns regulars;
    public final Columns statics;

    private PartitionColumns(Columns regulars, Columns statics)
    {
        this.regulars = regulars;
        this.statics = statics;
    }

    public boolean contains(ColumnDefinition column)
    {
        return column.isStatic() ? statics.contains(column) : regulars.contains(column);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Set<ColumnDefinition> regularColumns;
        private Set<ColumnDefinition> staticColumns;

        public Builder add(ColumnDefinition c)
        {
            if (c.isStatic())
            {
                if (staticColumns == null)
                    staticColumns = new HashSet<>();
                staticColumns.add(c);
            }
            else
            {
                if (regularColumns == null)
                    regularColumns = new HashSet<>();
                regularColumns.add(c);
            }
            return this;
        }

        public int added()
        {
            return (regularColumns == null ? 0 : regularColumns.size())
                 + (staticColumns == null ? 0 : staticColumns.size());
        }

        public Builder addAll(Iterable<ColumnDefinition> columns)
        {
            for (ColumnDefinition c : columns)
                add(c);
            return this;
        }

        public Builder addAll(PartitionColumns columns)
        {
            if (regularColumns == null && columns.regulars.size() > 0)
                regularColumns = new HashSet<>();

            for (ColumnDefinition c : columns.regulars)
                regularColumns.add(c);

            if (staticColumns == null && columns.statics.size() > 0)
                staticColumns = new HashSet<>();

            for (ColumnDefinition c : columns.statics)
                staticColumns.add(c);

            return this;
        }

        public PartitionColumns build()
        {
            return new PartitionColumns(regularColumns == null ? Columns.NONE : Columns.from(regularColumns),
                                        staticColumns == null ? Columns.NONE : Columns.from(staticColumns));
        }
    }
}
