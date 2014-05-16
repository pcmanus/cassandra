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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Static helper methods and classes for tuples.
 */
public class Tuples
{
    private static final Logger logger = LoggerFactory.getLogger(Tuples.class);

    private Tuples() {}

    public static ColumnSpecification componentSpecOf(ColumnSpecification column, int component)
    {
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(String.format("%s[%d]", column.name, component), true),
                                       ((TupleType)column.type).type(component));
    }

    public static class Literal implements Term.Raw
    {
        public final List<Term.Raw> values;

        public Literal(List<Term.Raw> values)
        {
            this.values = values;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            TupleType tt = (TupleType)receiver.type;
            boolean allTerminal = true;
            List<Term> ts = new ArrayList<>(values.size());
            for (int i = 0; i < tt.size(); i++)
            {
                Term value = values.get(i).prepare(keyspace, componentSpecOf(receiver, i));

                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                ts.add(value);
            }
            DelayedValue value = new DelayedValue(ts);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof TupleType))
                throw new InvalidRequestException(String.format("Invalid tuple type literal for %s of type %s", receiver, receiver.type.asCQL3Type()));

            TupleType tt = (TupleType)receiver.type;
            for (int i = 0; i < tt.size(); i++)
            {
                Term.Raw value = values.get(i);
                ColumnSpecification spec = componentSpecOf(receiver, i);
                if (!value.isAssignableTo(keyspace, spec))
                    throw new InvalidRequestException(String.format("Invalid tuple literal for %s: component %d is not of type %s", receiver, i, spec.type.asCQL3Type()));
            }
        }

        public boolean isAssignableTo(String keyspace, ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(keyspace, receiver);
                return true;
            }
            catch (InvalidRequestException e)
            {
                return false;
            }
        }

        @Override
        public String toString()
        {
            return tupleToString(values);
        }
    }

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    public static class DelayedValue extends Term.NonTerminal
    {
        private final List<Term> values;

        public DelayedValue(List<Term> values)
        {
            this.values = values;
        }

        public boolean containsBindMarker()
        {
            for (Term t : values)
                if (t.containsBindMarker())
                    return true;
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            for (int i = 0; i < values.size(); i++)
                values.get(i).collectMarkerSpecification(boundNames);
        }

        private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException
        {
            // Inside tuples, we must force the serialization of collections whatever the protocol version is in
            // use since we're going to store directly that serialized value.
            options = options.withProtocolVersion(3);

            ByteBuffer[] buffers = new ByteBuffer[values.size()];
            for (int i = 0; i < values.size(); i++)
            {
                ByteBuffer buffer = values.get(i).bindAndGet(options);
                if (buffer == null)
                    throw new InvalidRequestException("null is not supported inside tuple literals");

                buffers[i] = buffer;
            }
            return buffers;
        }

        public Constants.Value bind(QueryOptions options) throws InvalidRequestException
        {
            return new Constants.Value(bindAndGet(options));
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            return TupleType.buildValue(bindInternal(options));
        }
    }

    public static String tupleToString(List<?> items)
    {

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < items.size(); i++)
        {
            sb.append(items.get(i));
            if (i < items.size() - 1)
                sb.append(", ");
        }
        sb.append(')');
        return sb.toString();
    }
}
