package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareResponse
{
    public static final PrepareResponseSerializer serializer = new PrepareResponseSerializer();

    public final boolean promised;
    public final Commit inProgressCommit;
    public final Commit mostRecentCommit;

    public PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit)
    {
        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s)", promised, mostRecentCommit, inProgressCommit);
    }

    public static class PrepareResponseSerializer implements IVersionedSerializer<PrepareResponse>
    {
        public void serialize(PrepareResponse response, DataOutput out, int version) throws IOException
        {
            out.writeBoolean(response.promised);
            Commit.serializer.serialize(response.inProgressCommit, out, version);
            Commit.serializer.serialize(response.mostRecentCommit, out, version);
        }

        public PrepareResponse deserialize(DataInput in, int version) throws IOException
        {
            return new PrepareResponse(in.readBoolean(),
                                       Commit.serializer.deserialize(in, version),
                                       Commit.serializer.deserialize(in, version));
        }

        public long serializedSize(PrepareResponse response, int version)
        {
            return 1
                   + Commit.serializer.serializedSize(response.inProgressCommit, version)
                   + Commit.serializer.serializedSize(response.mostRecentCommit, version);
        }
    }
}
