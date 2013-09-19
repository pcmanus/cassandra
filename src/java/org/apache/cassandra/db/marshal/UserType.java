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
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.Pair;

/**
 * A user defined type.
 *
 * The serialized format and sorting is exactly the one of CompositeType, but
 * we keep additional metadata (the name of the type and the names
 * of the columns).
 */
public class UserType extends CompositeType
{
    public final ByteBuffer name;
    public final List<ByteBuffer> columnNames;

    public UserType(ByteBuffer name, List<ByteBuffer> columnNames, List<AbstractType<?>> types)
    {
        super(types);
        this.name = name;
        this.columnNames = columnNames;
    }

    public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        Pair<ByteBuffer, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
        ByteBuffer name = params.left;
        List<ByteBuffer> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType> p : params.right)
        {
            columnNames.add(p.left);
            columnTypes.add(p.right);
        }
        return new UserType(name, columnNames, columnTypes);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(name, columnNames, types);
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof UserType))
            return false;

        UserType that = (UserType)o;
        return name.equals(that.name) && columnNames.equals(that.columnNames) && types.equals(that.types);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        /*
         * The rules are:
         *  1. We allow 'upgrading' from a CompositeType, provided the types match.
         *  2. We allow adding new components to an existing UserType.
         *  3. We allow renaming a column/field name.
         */
        if (previous instanceof UserType)
        {
            UserType ut = (UserType)previous;
            if (!name.equals(ut.name) || ut.types.size() > types.size())
                return false;

            for (int i = 0; i < ut.types.size(); i++)
            {
                AbstractType tprev = ut.types.get(i);
                AbstractType tnew = types.get(i);
                if (!tnew.isCompatibleWith(tprev))
                    return false;
            }
        }

        if (!(previous instanceof CompositeType))
            return false;

        CompositeType cp = (CompositeType)previous;
        if (cp.types.size() > types.size())
            return false;

        for (int i = 0; i < cp.types.size(); i++)
        {
            AbstractType tprev = cp.types.get(i);
            AbstractType tnew = types.get(i);
            if (!tnew.isCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyUserTypeParameters(name, columnNames, types);
    }
}
