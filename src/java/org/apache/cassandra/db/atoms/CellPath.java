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
package org.apache.cassandra.db.atoms;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A path for a cell belonging to a complex column type (non-frozen collection or UDT).
 */
public interface CellPath
{
    public static final Serializer serializer = new Serializer();

    public static class Serializer
    {
        public void serialize(CellPath path, DataOutputPlus out)
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public CellPath deserialize(DataInput in)
        {
            // TODO
            throw new UnsupportedOperationException();
        }
    }
}
