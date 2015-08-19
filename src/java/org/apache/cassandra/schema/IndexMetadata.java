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

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.FBUtilities;

/**
 * An immutable representation of secondary index metadata.
 */
public final class IndexMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMetadata.class);

    public enum IndexType
    {
        KEYS, CUSTOM, COMPOSITES
    }

    public enum TargetType
    {
        COLUMN, ROW
    }

    public final String name;
    public final IndexType indexType;
    public final TargetType targetType;
    public final Map<String, String> options;
    public final Set<ColumnIdentifier> columns;

    private IndexMetadata(String name,
                          Map<String, String> options,
                          IndexType indexType,
                          TargetType targetType,
                          Set<ColumnIdentifier> columns)
    {
        this.name = name;
        this.options = options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
        this.indexType = indexType;
        this.targetType = targetType;
        this.columns = columns == null ? ImmutableSet.of() : ImmutableSet.copyOf(columns);
    }

    public static IndexMetadata legacyIndex(ColumnIdentifier column,
                                              String name,
                                              IndexType type,
                                              Map<String, String> options)
    {
        return new IndexMetadata(name, options, type, TargetType.COLUMN, Collections.singleton(column));
    }

    public static IndexMetadata legacyIndex(ColumnDefinition column,
                                              String name,
                                              IndexType type,
                                              Map<String, String> options)
    {
        return legacyIndex(column.name, name, type, options);
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.matches("\\w+");
    }

    // these will go away as part of #9459 as we enable real per-row indexes
    public static String getDefaultIndexName(String cfName, ColumnIdentifier columnName)
    {
        return (cfName + "_" + columnName + "_idx").replaceAll("\\W", "");
    }

    public void validate()
    {
        if (!isNameValid(name))
            throw new ConfigurationException("Illegal index name " + name);

        if (indexType == null)
            throw new ConfigurationException("Index type is null for index " + name);

        if (targetType == null)
            throw new ConfigurationException("Target type is null for index " + name);

        if (indexType == IndexMetadata.IndexType.CUSTOM)
        {
            if (options == null || !options.containsKey(IndexTarget.CUSTOM_INDEX_OPTION_NAME))
                throw new ConfigurationException(String.format("Required option missing for index %s : %s",
                                                               name, IndexTarget.CUSTOM_INDEX_OPTION_NAME));
            String className = options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
            Class<Index> indexerClass = FBUtilities.classForName(className, "custom indexer");
            if(!Index.class.isAssignableFrom(indexerClass))
                throw new ConfigurationException(String.format("Specified Indexer class (%s) does not implement the Indexer interface", className));
            validateCustomIndexOptions(indexerClass, options);
        }
    }

    private void validateCustomIndexOptions(Class<? extends Index> indexerClass, Map<String, String> options) throws ConfigurationException
    {
        try
        {
            Map<String, String> filteredOptions =
                Maps.filterKeys(options,key -> !key.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            if (filteredOptions.isEmpty())
                return;

            Map<?,?> unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class).invoke(null, filteredOptions);
            if (!unknownOptions.isEmpty())
                throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", unknownOptions.keySet(), indexerClass.getSimpleName()));
        }
        catch (NoSuchMethodException e)
        {
            logger.info("Indexer {} does not have a static validateOptions method. Validation ignored",
                        indexerClass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();
            throw new ConfigurationException("Failed to validate custom indexer options: " + options);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate custom indexer options: " + options);
        }
    }

    public ColumnDefinition indexedColumn(CFMetaData cfm)
    {
       return cfm.getColumnDefinition(columns.iterator().next());
    }

    public boolean isCustom()
    {
        return indexType == IndexType.CUSTOM;
    }

    public boolean isKeys()
    {
        return indexType == IndexType.KEYS;
    }

    public boolean isComposites()
    {
        return indexType == IndexType.COMPOSITES;
    }

    public int hashCode()
    {
        return Objects.hashCode(name, indexType, targetType, options, columns);
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexMetadata))
            return false;

        IndexMetadata other = (IndexMetadata)obj;

        return Objects.equal(name, other.name)
            && Objects.equal(indexType, other.indexType)
            && Objects.equal(targetType, other.targetType)
            && Objects.equal(options, other.options)
            && Objects.equal(columns, other.columns);
    }

    public String toString()
    {
        return new ToStringBuilder(this)
            .append("name", name)
            .append("indexType", indexType)
            .append("targetType", targetType)
            .append("columns", columns)
            .append("options", options)
            .build();
    }
}
