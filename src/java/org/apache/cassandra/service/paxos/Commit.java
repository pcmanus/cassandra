package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class Commit
{
    public static final CommitSerializer serializer = new CommitSerializer();

    public final UUID ballot;
    public final PartitionUpdate update;

    public Commit(UUID ballot, PartitionUpdate update)
    {
        assert ballot != null;
        assert update != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(DecoratedKey key, CFMetaData metadata, UUID ballot)
    {
        return new Commit(ballot, new PartitionUpdate(metadata, key, metadata.partitionColumns(), 1));
    }

    public static Commit newProposal(UUID ballot, PartitionUpdate update)
    {
        return new Commit(ballot, updatesWithPaxosTime(update, ballot));
    }

    public static Commit emptyCommit(DecoratedKey key, CFMetaData metadata)
    {
        // TODO: coul
        return new Commit(UUIDGen.minTimeUUID(0), new PartitionUpdate(metadata, key, PartitionColumns.NONE, 0));
    }

    public boolean isAfter(Commit other)
    {
        return ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean hasBallot(UUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    public Mutation makeMutation()
    {
        assert update != null;
        return new Mutation(update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Commit commit = (Commit) o;

        if (!ballot.equals(commit.ballot)) return false;
        if (!update.equals(commit.update)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ballot, update);
    }

    private static PartitionUpdate updatesWithPaxosTime(PartitionUpdate updates, UUID ballot)
    {
        // TODO
        throw new UnsupportedOperationException();
        //ColumnFamily cf = updates.cloneMeShallow();
        //long t = UUIDGen.microsTimestamp(ballot);
        //// For the tombstones, we use t-1 so that when insert a collection literall, the range tombstone that deletes the previous values of
        //// the collection and we want that to have a lower timestamp and our new values. Since tombstones wins over normal insert, using t-1
        //// should not be a problem in general (see #6069).
        //cf.deletionInfo().updateAllTimestamp(t-1);
        //for (Cell cell : updates)
        //    cf.addAtom(cell.withUpdatedTimestamp(t));
        //return cf;
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s)", ballot, update);
    }

    // TODO
    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            //ByteBufferUtil.writeWithShortLength(commit.key, out);
            //UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            //ColumnFamily.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
            //return new Commit(ByteBufferUtil.readWithShortLength(in),
            //                  UUIDSerializer.serializer.deserialize(in, version),
            //                  ColumnFamily.serializer.deserialize(in,
            //                                                      ArrayBackedSortedColumns.factory,
            //                                                      ColumnSerializer.Flag.LOCAL,
            //                                                      version));
        }

        public long serializedSize(Commit commit, int version)
        {
            throw new UnsupportedOperationException();
            //return 2 + commit.key.remaining()
            //       + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
            //       + ColumnFamily.serializer.serializedSize(commit.update, version);
        }
    }
}
