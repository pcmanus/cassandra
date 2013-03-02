package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.UUIDSerializer;

public class ProposeRequest
{
    public static final ProposeRequestSerializer serializer = new ProposeRequestSerializer();

    public final UUID ballot;
    public final Row proposal;

    public ProposeRequest(UUID ballot, Row proposal)
    {
        this.ballot = ballot;
        this.proposal = proposal;
    }

    public static class ProposeRequestSerializer implements IVersionedSerializer<ProposeRequest>
    {
        public void serialize(ProposeRequest proposal, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(proposal.ballot, out, version);
            Row.serializer.serialize(proposal.proposal, out, version);
        }

        public ProposeRequest deserialize(DataInput in, int version) throws IOException
        {
            return new ProposeRequest(UUIDSerializer.serializer.deserialize(in, version),
                                     Row.serializer.deserialize(in, version));
        }

        public long serializedSize(ProposeRequest proposal, int version)
        {
            return UUIDSerializer.serializer.serializedSize(proposal.ballot, version)
                   + Row.serializer.serializedSize(proposal.proposal, version);
        }
    }
}
