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

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * An immutable sorted list of Column.
 */
public class Columns implements Iterable<ColumnDefinition>
{
    public static Columns NONE = new Columns(new ColumnDefinition[0]);

    private final ColumnDefinition[] columns;

    private Columns(ColumnDefinition[] columns)
    {
        this.columns = columns;
    }

    public static Columns of(ColumnDefinition c)
    {
        return new Columns(new ColumnDefinition[]{ c });
    }

    public static Columns from(Set<ColumnDefinition> s)
    {
        ColumnDefinition[] columns = s.toArray(new ColumnDefinition[s.size()]);
        Arrays.sort(columns);
        return new Columns(columns);
    }

    public boolean isEmpty()
    {
        return columns.length == 0;
    }

    public int size()
    {
        return columns.length;
    }

    public ColumnDefinition get(int i)
    {
        return columns[i];
    }

    public int indexOf(ColumnDefinition c)
    {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].name.equals(c.name))
                return i;
        return -1;
    }

    public boolean contains(ColumnDefinition c)
    {
        return indexOf(c) >= 0;
    }

    public int indexOfComplex(ColumnDefinition c)
    {
        int idx = 0;
        for (int i = 0; i < columns.length; i++)
        {
            if (!columns[i].isComplex())
                continue;

            if (columns[i].name.equals(c.name))
                return idx;

            ++idx;
        }
        return -1;
    }

    public Columns mergeTo(Columns other)
    {
        if (this == other)
            return this;

        int i = 0, j = 0;
        int size = 0;
        while (i < columns.length && j < other.columns.length)
        {
            ++size;
            int cmp = columns[i].compareTo(other.columns[j]);
            if (cmp == 0)
            {
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                ++i;
            }
            else
            {
                ++j;
            }
        }

        // If every element was always counted on both array, we have the same
        // arrays for the first min elements
        if (i == size && j == size)
        {
            // We've exited because of either c1 or c2 (or both). The array that
            // made us stop is thus a subset of the 2nd one, return that array.
            return i == columns.length ? other : this;
        }

        size += i == columns.length ? other.columns.length - j : columns.length - i;
        ColumnDefinition[] result = new ColumnDefinition[size];
        i = 0;
        j = 0;
        for (int k = 0; k < size; k++)
        {
            int cmp = columns[i].compareTo(other.columns[j]);
            if (cmp == 0)
            {
                result[k] = columns[i++];
                ++j;
            }
            else if (cmp < 0)
            {
                result[k] = columns[i++];
            }
            else
            {
                result[k] = other.columns[j++];
            }
        }
        return new Columns(result);
    }

    public Iterator<ColumnDefinition> iterator()
    {
        return Iterators.forArray(columns);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        Set<ColumnDefinition> columns;
        Set<ColumnDefinition> staticColumns;

        public Builder add(ColumnDefinition c)
        {
            if (c.isStatic())
            {
                if (staticColumns == null)
                    staticColumns = new TreeSet<>();
                staticColumns.add(c);
            }
            else
            {
                if (columns == null)
                    columns = new TreeSet<>();
                columns.add(c);
            }
            return this;
        }

        public Builder addAll(Iterable<ColumnDefinition> columns)
        {
            for (ColumnDefinition c : columns)
                add(c);
            return this;
        }

        public Columns regularColumns()
        {
            if (columns == null)
                return Columns.NONE;

            return new Columns(columns.toArray(new ColumnDefinition[columns.size()]));
        }

        public Columns staticColumns()
        {
            if (staticColumns == null)
                return Columns.NONE;

            return new Columns(staticColumns.toArray(new ColumnDefinition[staticColumns.size()]));
        }
    }
}
