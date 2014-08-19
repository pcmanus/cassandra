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

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class Functions
{
    private static final Logger logger = LoggerFactory.getLogger(Functions.class);

    // We special case the token function because that's the only function whose argument types actually
    // depend on the table on which the function is called. Because it's the sole exception, it's easier
    // to handle it as a special case.
    private static final FunctionName TOKEN_FUNCTION_NAME = new FunctionName("token");

    private static final String SELECT_UDFS = "SELECT * FROM " + Keyspace.SYSTEM_KS + '.' + SystemKeyspace.SCHEMA_FUNCTIONS_CF;

    private Functions() {}

    private static final ArrayListMultimap<FunctionName, Function> declared = ArrayListMultimap.create();

    static
    {
        declare(TimeuuidFcts.nowFct);
        declare(TimeuuidFcts.minTimeuuidFct);
        declare(TimeuuidFcts.maxTimeuuidFct);
        declare(TimeuuidFcts.dateOfFct);
        declare(TimeuuidFcts.unixTimestampOfFct);
        declare(UuidFcts.uuidFct);

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // Note: because text and varchar ends up being synonimous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type == CQL3Type.Native.VARCHAR || type == CQL3Type.Native.BLOB)
                continue;

            declare(BytesConversionFcts.makeToBlobFunction(type.getType()));
            declare(BytesConversionFcts.makeFromBlobFunction(type.getType()));
        }
        declare(BytesConversionFcts.VarcharAsBlobFct);
        declare(BytesConversionFcts.BlobAsVarcharFact);
    }

    private static void declare(Function fun)
    {
        declared.put(fun.name(), fun);
    }

    /**
     * Loading existing UDFs from the schema.
     */
    public static void loadUDFFromSchema()
    {
        logger.debug("Loading UDFs");
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(SELECT_UDFS))
        {
            try
            {
                addFunction(UDFunction.fromSchema(row));
            }
            catch (InvalidRequestException e)
            {
                // fromSchema only throws if it can't create the function. This could happen if a UDF was registered,
                // but the class implementing it is not in the classpatch this time around for instance. In that case,
                // log the error but skip the function otherwise as we don't want to break schema updates for that.
                logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)",
                                           UDFunction.getName(row)), e);
            }
        }
    }

    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                                       receiverCf,
                                       new ColumnIdentifier("arg" + i +  "(" + fun.name().toString().toLowerCase() + ")", true),
                                       fun.argTypes().get(i));
    }

    public static Function get(String keyspace,
                               FunctionName name,
                               List<? extends AssignementTestable> providedArgs,
                               String receiverKs,
                               String receiverCf)
    throws InvalidRequestException
    {
        if (name.equals(TOKEN_FUNCTION_NAME))
            return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));

        List<Function> candidates = declared.get(name);
        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.get(0);
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
        }

        Function candidate = null;
        for (Function toTest : candidates)
        {
            if (!isValidType(keyspace, toTest, providedArgs, receiverKs, receiverCf))
                continue;

            if (candidate == null)
                candidate = toTest;
            else
                throw new InvalidRequestException(String.format("Ambiguous call to function %s (can match both type signature %s and %s): use type casts to disambiguate",
                                                                name, candidate, toTest));
        }
        if (candidate == null)
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signature matches (known type signatures: %s)",
                                                            name, toString(candidates)));
        return candidate;
    }

    public static List<Function> find(FunctionName name)
    {
        return declared.get(name);
    }

    public static Function find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        for (Function f : declared.get(name))
        {
            if (f.argTypes().equals(argTypes))
                return f;
        }
        return null;
    }

    // This method and isValidType are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignementTestable> providedArgs,
                                      String receiverKs,
                                      String receiverCf)
    throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argTypes().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignementTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.isAssignableTo(keyspace, expected))
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static boolean isValidType(String keyspace,
                                       Function fun,
                                       List<? extends AssignementTestable> providedArgs,
                                       String receiverKs,
                                       String receiverCf)
    throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            return false;

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignementTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.isAssignableTo(keyspace, expected))
                return false;
        }
        return true;
    }

    private static String toString(List<Function> funs)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < funs.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(funs.get(i));
        }
        return sb.toString();
    }

    // This is *not* thread safe but is only called in DefsTables that is synchronized.
    public static void addFunction(UDFunction fun)
    {
        // We shouldn't get there unless that function don't exist
        assert find(fun.name(), fun.argTypes()) == null;
        declare(fun);
    }

    // Same remarks than for addFunction
    public static void removeFunction(FunctionName name, List<AbstractType<?>> argsTypes)
    {
        Function old = find(name, argsTypes);
        assert old != null && !old.isNative();
        declared.remove(old.name(), old);
    }

    // Same remarks than for addFunction
    public static void replaceFunction(UDFunction fun)
    {
        removeFunction(fun.name(), fun.argTypes());
        addFunction(fun);
    }
}
