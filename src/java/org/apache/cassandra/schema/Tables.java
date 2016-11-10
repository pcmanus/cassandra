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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import static com.google.common.collect.Iterables.filter;

/**
 * An immutable container for a keyspace's Tables.
 */
public final class Tables implements Iterable<TableMetadata>
{
    private final ImmutableMap<String, TableMetadata> tables;

    private Tables(Builder builder)
    {
        tables = builder.tables.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Tables none()
    {
        return builder().build();
    }

    public static Tables of(TableMetadata... tables)
    {
        return builder().add(tables).build();
    }

    public static Tables of(Iterable<TableMetadata> tables)
    {
        return builder().add(tables).build();
    }

    public Iterator<TableMetadata> iterator()
    {
        return tables.values().iterator();
    }

    public int size()
    {
        return tables.size();
    }

    /**
     * Get the table with the specified name
     *
     * @param name a non-qualified table name
     * @return an empty {@link Optional} if the table name is not found; a non-empty optional of {@link TableMetadataRef} otherwise
     */
    public Optional<TableMetadata> get(String name)
    {
        return Optional.ofNullable(tables.get(name));
    }

    /**
     * Get the table with the specified name
     *
     * @param name a non-qualified table name
     * @return null if the table name is not found; the found {@link TableMetadataRef} otherwise
     */
    @Nullable
    public TableMetadata getNullable(String name)
    {
        return tables.get(name);
    }

    /**
     * Create a Tables instance with the provided table added
     */
    public Tables with(TableMetadata table)
    {
        if (get(table.table).isPresent())
            throw new IllegalStateException(String.format("Table %s already exists", table.table));

        return builder().add(this).add(table).build();
    }

    public Tables withSwapped(TableMetadata table)
    {
        return without(table.table).with(table);
    }

    /**
     * Creates a Tables instance with the table with the provided name removed
     */
    public Tables without(String name)
    {
        TableMetadata table =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Table %s doesn't exists", name)));

        return builder().add(filter(this, t -> t != table)).build();
    }

    MapDifference<UUID, TableMetadata> diff(Tables other)
    {
        Map<UUID, TableMetadata> thisTables = new HashMap<>();
        this.forEach(t -> thisTables.put(t.id, t));

        Map<UUID, TableMetadata> otherTables = new HashMap<>();
        other.forEach(t -> otherTables.put(t.id, t));

        return Maps.difference(thisTables, otherTables);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Tables && tables.equals(((Tables) o).tables));
    }

    @Override
    public int hashCode()
    {
        return tables.hashCode();
    }

    @Override
    public String toString()
    {
        return tables.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, TableMetadata> tables = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Tables build()
        {
            return new Tables(this);
        }

        public Builder add(TableMetadata table)
        {
            tables.put(table.table, table);
            return this;
        }

        public Builder add(TableMetadata... tables)
        {
            for (TableMetadata table : tables)
                add(table);
            return this;
        }

        public Builder add(Iterable<TableMetadata> tables)
        {
            tables.forEach(this::add);
            return this;
        }
    }
}
