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
    public final UUID inProgressBallot;
    public final ColumnFamily inProgressUpdates;
    public final Commit mostRecentCommit;

    public PrepareResponse(boolean promised, UUID inProgressBallot, ColumnFamily inProgressUpdates, Commit mostRecentCommit)
    {
        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressBallot = inProgressBallot;
        this.inProgressUpdates = inProgressUpdates;
    }

    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s, %s)",
                             promised, mostRecentCommit, inProgressBallot, inProgressUpdates);
    }

    public static class PrepareResponseSerializer implements IVersionedSerializer<PrepareResponse>
    {
        public void serialize(PrepareResponse response, DataOutput out, int version) throws IOException
        {
            out.writeBoolean(response.promised);

            UUIDSerializer.serializer.serialize(response.inProgressBallot, out, version);
            ColumnFamily.serializer.serialize(response.inProgressUpdates, out, version);

            Commit.serializer.serialize(response.mostRecentCommit, out, version);
        }

        public PrepareResponse deserialize(DataInput in, int version) throws IOException
        {
            return new PrepareResponse(in.readBoolean(),
                                       UUIDSerializer.serializer.deserialize(in, version),
                                       ColumnFamily.serializer.deserialize(in, version),
                                       Commit.serializer.deserialize(in, version));
        }

        public long serializedSize(PrepareResponse response, int version)
        {
            return 1
                   + UUIDSerializer.serializer.serializedSize(response.inProgressBallot, version)
                   + ColumnFamily.serializer.serializedSize(response.inProgressUpdates, version)
                   + Commit.serializer.serializedSize(response.mostRecentCommit, version);
        }
    }
}
