package org.apache.cassandra.service.paxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PrepareVerbHandler implements IVerbHandler<PrepareRequest>
{
    public void doVerb(MessageIn<PrepareRequest> message, int id)
    {
        PaxosState state = PaxosState.stateFor(message.payload.key);

        PrepareResponse response = state.prepare(message.payload.ballot);
        MessageOut<PrepareResponse> reply = new MessageOut<PrepareResponse>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareResponse.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
