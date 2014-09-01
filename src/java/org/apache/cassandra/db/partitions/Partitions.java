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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Atom;

public abstract class Partitions
{
    private Partitions() {}

    public interface Writer
    {
        public void newPartition(DecoratedKey partitionKey, DeletionTime partitionLevelDeletion);
        public void newAtom(Atom atom);
        public void partitionDone();
    }

    // From Row.java
    //public static class RowSerializer implements IVersionedSerializer<Row>
    //{
    //    public void serialize(Row row, DataOutputPlus out, int version) throws IOException
    //    {
    //        ByteBufferUtil.writeWithShortLength(row.key.getKey(), out);
    //        ColumnFamily.serializer.serialize(row.cf, out, version);
    //    }

    //    public Row deserialize(DataInput in, int version, ColumnSerializer.Flag flag) throws IOException
    //    {
    //        return new Row(StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in)),
    //                       ColumnFamily.serializer.deserialize(in, flag, version));
    //    }

    //    public Row deserialize(DataInput in, int version) throws IOException
    //    {
    //        return deserialize(in, version, ColumnSerializer.Flag.LOCAL);
    //    }

    //    public long serializedSize(Row row, int version)
    //    {
    //        int keySize = row.key.getKey().remaining();
    //        return TypeSizes.NATIVE.sizeof((short) keySize) + keySize + ColumnFamily.serializer.serializedSize(row.cf, TypeSizes.NATIVE, version);
    //    }
    //}
}
