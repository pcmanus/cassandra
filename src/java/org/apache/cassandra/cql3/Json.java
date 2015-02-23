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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.codehaus.jackson.map.ObjectMapper;


/** Term-related classes for INSERT JSON support. */
public class Json
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public interface Raw
    {
        public Prepared prepareAndCollectMarkers(CFMetaData metadata, Collection<ColumnDefinition> receivers, VariableSpecifications boundNames);
    }

    public static abstract class Prepared
    {
        private final String keyspace;

        protected Prepared(String keyspace)
        {
            this.keyspace = keyspace;
        }

        protected abstract Term.Raw getRawTermForColumn(ColumnDefinition def);

        public Term getPrimarykeyValueFor(ColumnDefinition def)
        {
            // Note that we know we don't have to call collectMarkerSpecification since it has already been collected
            return getRawTermForColumn(def).prepare(keyspace, def);
        }

        public Operation getSetOperationFor(ColumnDefinition def)
        {
            // Note that we know we don't have to call collectMarkerSpecification on the operation since we have
            // already collected all we need.
            return new Operation.SetValue(getRawTermForColumn(def)).prepare(keyspace, def);
        }
    }

    /**
     * Represents a literal JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable JSON '{"key": 0, "col": 0}';
     */
    public static class Literal implements Raw
    {
        private final String text;

        public Literal(String text)
        {
            this.text = text;
        }

        public Prepared prepareAndCollectMarkers(CFMetaData metadata, Collection<ColumnDefinition> receivers, VariableSpecifications boundNames)
        {
            return new SimplePrepared(metadata.ksName, parseJson(text, receivers));
        }
    }

    /**
     * Represents a marker for a JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable JSON ?;
     */
    public static class Marker implements Raw
    {
        protected final int bindIndex;

        public Marker(int bindIndex)
        {
            this.bindIndex = bindIndex;
        }

        public Prepared prepareAndCollectMarkers(CFMetaData metadata, Collection<ColumnDefinition> receivers, VariableSpecifications boundNames)
        {
            boundNames.add(bindIndex, makeReceiver(metadata));
            return new DelayedPrepared(metadata.ksName, bindIndex, receivers);
        }

        private static ColumnSpecification makeReceiver(CFMetaData metadata) throws InvalidRequestException
        {
            ColumnIdentifier identifier = new ColumnIdentifier("[json]", true);
            return new ColumnSpecification(metadata.ksName, metadata.cfName, identifier, UTF8Type.instance);
        }
    }

    /**
     * A Term for a single (parsed) column value.
     *
     * Note that this is intrinsically an already prepared term, but
     * this still implements Term.Raw so that it can easily use to create
     * raw operations.
     */
    private static class ColumnValue extends Term.Terminal implements Term.Raw
    {
        private final ColumnDefinition column;
        private final Object parsedJsonValue;

        public ColumnValue(Object parsedJsonValue, ColumnDefinition column)
        {
            this.parsedJsonValue = parsedJsonValue;
            this.column = column;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver)
        {
            return this;
        }

        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return TestResult.NOT_ASSIGNABLE;
        }

        public ByteBuffer get(int protocolVersion) throws InvalidRequestException
        {
            try
            {
                return column.type.fromJSONObject(parsedJsonValue, protocolVersion);
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Error decoding JSON value for %s: %s", column.name, exc.getMessage()));
            }
        }
    }

    /**
     * A non-terminal Term for a single column value.
     *
     * As with {@code ColumValue}, this is intrinsically a prepared term but
     * implements Terms.Raw for convenience.
     */
    private static class DelayedValue extends Term.NonTerminal implements Term.Raw
    {
        private final DelayedPrepared prepared;
        private final ColumnDefinition column;

        public DelayedValue(DelayedPrepared prepared, ColumnDefinition column)
        {
            this.prepared = prepared;
            this.column = column;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver)
        {
            return this;
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            // We've already collected what we should (and in practice this method is never called).
        }

        public boolean containsBindMarker()
        {
            return true;
        }

        public Term.Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            prepared.bind(options);
            Object value = prepared.getValue(column);
            return value == null ? null : new ColumnValue(value, column);
        }
    }

    private static void handleCaseSensitivity(Map<String, Object> valueMap)
    {
        for (String mapKey : new ArrayList<>(valueMap.keySet()))
        {
            // if it's surrounded by quotes, remove them and preserve the case
            if (mapKey.startsWith("\"") && mapKey.endsWith("\""))
            {
                valueMap.put(mapKey.substring(1, mapKey.length() - 1), valueMap.remove(mapKey));
                continue;
            }

            // otherwise, lowercase it if needed
            String lowered = mapKey.toLowerCase(Locale.US);
            if (!mapKey.equals(lowered))
                valueMap.put(lowered, valueMap.remove(mapKey));
        }
    }

    private static Map<ColumnIdentifier, Object> parseJson(String jsonString, Collection<ColumnDefinition> expectedReceivers)
    throws InvalidRequestException
    {
        try
        {
            Map<String, Object> valueMap = objectMapper.readValue(jsonString, Map.class);

            if (valueMap == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            handleCaseSensitivity(valueMap);

            Map<ColumnIdentifier, Object> columnMap = new HashMap<>(expectedReceivers.size());
            for (ColumnDefinition def : expectedReceivers)
                columnMap.put(def.name, valueMap.remove(def.name.toString()));

            if (!valueMap.isEmpty())
                throw new InvalidRequestException(String.format("JSON values map contains unrecognized column: %s",
                                                                valueMap.keySet().iterator().next()));

            return columnMap;
        }
        catch (IOException exc)
        {
            throw new InvalidRequestException(String.format("Could not decode JSON string as a map: %s. (String was: %s)",
                                                            exc.toString(), jsonString));
        }
        catch (MarshalException exc)
        {
            throw new InvalidRequestException(exc.getMessage());
        }
    }

    private static class SimplePrepared extends Prepared
    {
        private final Map<ColumnIdentifier, Object> columnMap;

        public SimplePrepared(String keyspace, Map<ColumnIdentifier, Object> columnMap)
        {
            super(keyspace);
            this.columnMap = columnMap;
        }

        protected Term.Raw getRawTermForColumn(ColumnDefinition def)
        {
            Object value = columnMap.get(def.name);
            if (value == null)
                return Constants.NULL_LITERAL;

            return new ColumnValue(value, def);
        }
    }

    private static class DelayedPrepared extends Prepared
    {
        private final int bindIndex;
        private final Collection<ColumnDefinition> columns;

        private Map<ColumnIdentifier, Object> columnMap;

        public DelayedPrepared(String keyspace, int bindIndex, Collection<ColumnDefinition> columns)
        {
            super(keyspace);
            this.bindIndex = bindIndex;
            this.columns = columns;
        }

        protected Term.Raw getRawTermForColumn(ColumnDefinition def)
        {
            return new DelayedValue(this, def);
        }

        public void bind(QueryOptions options) throws InvalidRequestException
        {
            if (columnMap != null)
                return;

            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            columnMap = parseJson(UTF8Type.instance.getSerializer().deserialize(value), columns);
        }

        public Object getValue(ColumnDefinition def)
        {
            return columnMap.get(def.name);
        }
    }
}
