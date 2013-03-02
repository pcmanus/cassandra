package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareRequest
{
    public static PrepareRequestSerializer serializer = new PrepareRequestSerializer();

    public final UUID ballot;
    public final ByteBuffer key;

    public PrepareRequest(UUID ballot, ByteBuffer key)
    {
        this.ballot = ballot;
        this.key = key;
    }

    public static class PrepareRequestSerializer implements IVersionedSerializer<PrepareRequest>
    {
        public void serialize(PrepareRequest request, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            ByteBufferUtil.writeWithShortLength(request.key, out);
        }

        public PrepareRequest deserialize(DataInput in, int version) throws IOException
        {
            return new PrepareRequest(UUIDSerializer.serializer.deserialize(in, version),
                                      ByteBufferUtil.readWithShortLength(in));
        }

        public long serializedSize(PrepareRequest request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.ballot, version) + 2 + request.key.remaining();
        }
    }
}
