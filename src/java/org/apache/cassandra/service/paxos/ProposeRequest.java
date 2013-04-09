package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class ProposeRequest
{
    public static final ProposeRequestSerializer serializer = new ProposeRequestSerializer();

    public final ByteBuffer key;
    public final UUID ballot;
    public final ColumnFamily update;

    public ProposeRequest(ByteBuffer key, UUID ballot, ColumnFamily update)
    {
        assert key != null;
        assert ballot != null;
        assert update != null;

        this.key = key;
        this.ballot = ballot;
        this.update = update;
    }

    public static class ProposeRequestSerializer implements IVersionedSerializer<ProposeRequest>
    {
        public void serialize(ProposeRequest proposal, DataOutput out, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(proposal.key, out);
            UUIDSerializer.serializer.serialize(proposal.ballot, out, version);
            ColumnFamily.serializer.serialize(proposal.update, out, version);
        }

        public ProposeRequest deserialize(DataInput in, int version) throws IOException
        {
            return new ProposeRequest(ByteBufferUtil.readWithShortLength(in),
                                      UUIDSerializer.serializer.deserialize(in, version),
                                      ColumnFamily.serializer.deserialize(in, version));
        }

        public long serializedSize(ProposeRequest proposal, int version)
        {
            return 2 + proposal.key.remaining()
                   + UUIDSerializer.serializer.serializedSize(proposal.ballot, version)
                   + ColumnFamily.serializer.serializedSize(proposal.update, version);
        }
    }
}
