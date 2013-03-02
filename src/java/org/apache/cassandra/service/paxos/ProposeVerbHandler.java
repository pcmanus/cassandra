package org.apache.cassandra.service.paxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.BooleanSerializer;

public class ProposeVerbHandler implements IVerbHandler<ProposeRequest>
{
    public void doVerb(MessageIn<ProposeRequest> message, int id)
    {
        PaxosState state = PaxosState.stateFor(message.payload.proposal.key.key);

        Boolean response = state.propose(message.payload.ballot, message.payload.proposal);
        MessageOut<Boolean> reply = new MessageOut<Boolean>(MessagingService.Verb.REQUEST_RESPONSE, response, BooleanSerializer.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
