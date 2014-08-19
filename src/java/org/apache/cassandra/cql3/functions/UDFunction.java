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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;
    private final boolean deterministic;

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         AbstractType<?> returnType,
                         String language,
                         String body,
                         boolean deterministic)
    {
        super(name, argTypes, returnType);
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.deterministic = deterministic;
    }

    public static UDFunction create(FunctionName name,
                                    List<ColumnIdentifier> argNames,
                                    List<AbstractType<?>> argTypes,
                                    AbstractType<?> returnType,
                                    String language,
                                    String body,
                                    boolean deterministic)
    throws InvalidRequestException
    {
        switch (language)
        {
            case "class": return new ReflectionBasedUDF(name, argNames, argTypes, returnType, language, body, deterministic);
            default: throw new InvalidRequestException(String.format("Invalid language %s for '%s'", language, name));
        }
    }

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema 
    // we use a "signature" which is just a SHA-1 of it's argument types (we could replace that by
    // using a "signature" UDT that would be comprised of the function name and argument types,
    // which we could then use as clustering column. But as we haven't yet used UDT in system tables,
    // We'll left that decision to #6717).
    private static ByteBuffer computeSignature(List<AbstractType<?>> argTypes)
    {
        MessageDigest digest = FBUtilities.newMessageDigest("SHA-1");
        for (AbstractType<?> type : argTypes)
            digest.update(type.toString().getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(digest.digest());
    }

    public boolean isPure()
    {
        return deterministic;
    }

    public boolean isNative()
    {
        return false;
    }

    private static Mutation makeSchemaMutation(FunctionName name)
    {
        CompositeType kv = (CompositeType)CFMetaData.SchemaFunctionsCf.getKeyValidator();
        return new Mutation(Keyspace.SYSTEM_KS, kv.decompose(name.namespace, name.name));
    }

    // TODO: we should allow removing just one function, not all functions having a given name
    public static Mutation dropFromSchema(long timestamp, FunctionName fun)
    {
        Mutation mutation = makeSchemaMutation(fun);
        mutation.delete(SystemKeyspace.SCHEMA_FUNCTIONS_CF, timestamp);
        return mutation;
    }

    public Mutation toSchemaUpdate(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_CF);

        Composite prefix = CFMetaData.SchemaFunctionsCf.comparator.make(computeSignature(argTypes));
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");
        adder.add("return_type", returnType.toString());
        adder.add("language", language);
        adder.add("body", body);
        adder.add("deterministic", deterministic);

        for (int i = 0; i < argNames.size(); i++)
        {
            adder.addListEntry("argument_names", argNames.get(i).bytes);
            adder.addListEntry("argument_types", argTypes.get(i).toString());
        }

        return mutation;
    }

    static FunctionName getName(UntypedResultSet.Row row)
    {
        String namespace = row.getString("namespace");
        String name = row.getString("name");
        return new FunctionName(namespace, name);
    }

    public static UDFunction fromSchema(UntypedResultSet.Row row)
    throws InvalidRequestException
    {
        List<String> names = row.getList("argument_names", UTF8Type.instance);
        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<ColumnIdentifier> argNames = new ArrayList<>(names.size());
        for (String arg : names)
            argNames.add(new ColumnIdentifier(arg, true));

        List<AbstractType<?>> argTypes = new ArrayList<>(types.size());
        for (String type : types)
            argTypes.add(parseType(type));

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        boolean deterministic = row.getBoolean("deterministic");
        String language = row.getString("language");
        String body = row.getString("body");

        return create(getName(row), argNames, argTypes, returnType, language, body, deterministic);
    }

    private static AbstractType<?> parseType(String str)
    {
        // We only use this when reading the schema where we shouldn't get an error
        try
        {
            return TypeParser.parse(str);
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Map<ByteBuffer, UDFunction> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_FUNCTIONS_CF, row);
        Map<ByteBuffer, UDFunction> udfs = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
        {
            try
            {
                udfs.put(result.getBlob("signature"), fromSchema(result));
            }
            catch (InvalidRequestException e)
            {
                // fromSchema only throws if it can't create the function. This could happen if a UDF was registered,
                // but the class implementing it is not in the classpatch this time around for instance. In that case,
                // log the error but skip the function otherwise as we don't want to break schema updates for that.
                logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)",
                                           getName(result)), e);
            }
        }
        return udfs;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return Objects.equal(this.name, that.name)
            && Objects.equal(this.argNames, that.argNames)
            && Objects.equal(this.argTypes, that.argTypes)
            && Objects.equal(this.returnType, that.returnType)
            && Objects.equal(this.language, that.language)
            && Objects.equal(this.body, that.body)
            && Objects.equal(this.deterministic, that.deterministic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argNames, argTypes, returnType, language, body, deterministic);
    }
}
