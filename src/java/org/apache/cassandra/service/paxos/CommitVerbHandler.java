package org.apache.cassandra.service.paxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

public class CommitVerbHandler implements IVerbHandler<ProposeRequest>
{
    public void doVerb(MessageIn<ProposeRequest> message, int id)
    {
        PaxosState.commit(message.payload.key, message.payload.ballot, message.payload.update);
    }
}
