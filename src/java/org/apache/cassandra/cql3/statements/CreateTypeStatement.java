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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateTypeStatement extends SchemaAlteringStatement
{
    private final ColumnIdentifier name;
    private final List<ColumnIdentifier> columnNames = new ArrayList<>();
    private final List<CQL3Type> columnTypes = new ArrayList<>();
    private final boolean ifNotExists;

    public CreateTypeStatement(ColumnIdentifier name, boolean ifNotExists)
    {
        super();
        this.name = name;
        this.ifNotExists = ifNotExists;
    }

    public void addDefinition(ColumnIdentifier name, CQL3Type type)
    {
        columnNames.add(name);
        columnTypes.add(type);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        // We may want a slightly different permission?
        state.hasAllKeyspacesAccess(Permission.CREATE);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        UserType old = Schema.instance.userTypes.getType(name.key);
        if (old != null)
        {
            if (ifNotExists)
                return;
            else
                throw new InvalidRequestException(String.format("A user type of name %s already exists.", name));
        }
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.CREATED;
    }

    @Override
    public String keyspace()
    {
        // Not really a legit keyspace name, but SchemaAlteringStatement use that for schema change notification so
        // leave that for now; TODO: we need to change that
        return "";
    }

    private UserType createType()
    {
        List<ByteBuffer> names = new ArrayList<>(columnNames.size());
        for (ColumnIdentifier name : columnNames)
            names.add(name.key);

        List<AbstractType<?>> types = new ArrayList<>(columnTypes.size());
        for (CQL3Type type : columnTypes)
            types.add(type.getType());

        return new UserType(name.key, names, types);
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        // Can happen with ifNotExists
        if (Schema.instance.userTypes.getType(name.key) != null)
            return;

        MigrationManager.announceNewType(createType());
    }
}
