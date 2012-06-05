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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class ListType extends CollectionType
{
    // interning instances
    private static final Map<AbstractType<?>, ListType> instances = new HashMap<AbstractType<?>, ListType>();

    public final AbstractType<?> elements;

    public static ListType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized ListType getInstance(AbstractType<?> elements)
    {
        ListType t = instances.get(elements);
        if (t == null)
        {
            t = new ListType(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListType(AbstractType<?> elements)
    {
        super(Kind.LIST);
        this.elements = elements;
    }

    protected AbstractType<?> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    protected AbstractType<?> valueComparator()
    {
        return elements;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public void executeFunction(ColumnFamily cf, ColumnNameBuilder fullPath, Function fct, List<Term> args, UpdateParameters params) throws InvalidRequestException
    {
        assert fct == Function.APPEND;
        doAppend(cf, fullPath, args.get(0), params);
    }

    private void doAppend(ColumnFamily cf, ColumnNameBuilder builder, Term value, UpdateParameters params) throws InvalidRequestException
    {
        ByteBuffer name = builder.add(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())).build();
        cf.addColumn(params.makeColumn(name, value.getByteBuffer(elements, params.variables)));
    }

    public ByteBuffer serializeForThrift(Map<ByteBuffer, IColumn> columns)
    {
        List<Object> l = new ArrayList<Object>(columns.size());
        for (Map.Entry<ByteBuffer, IColumn> entry : columns.entrySet())
            l.add(elements.compose(entry.getValue().value()));
        return ByteBufferUtil.bytes(FBUtilities.json(l));
    }
}
