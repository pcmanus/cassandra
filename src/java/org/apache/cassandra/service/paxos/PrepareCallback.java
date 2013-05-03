package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.net.MessageIn;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse, PrepareSummary>
{
    private Commit mostRecentCommit;
    private Commit inProgressCommit;
    private Map<InetAddress, Commit> commitsByReplica = new HashMap<InetAddress, Commit>();

    public PrepareCallback(ByteBuffer key, CFMetaData metadata, int targets)
    {
        super(targets);
        // need to inject the right key in the empty commit so comparing with empty commits in the reply works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        inProgressCommit = Commit.emptyCommit(key, metadata);
    }

    protected synchronized boolean process(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        if (!response.promised)
        {
            // Set the future response, we know it's failed
            set(PrepareSummary.failedPrepare());
            return true;
        }

        commitsByReplica.put(message.from, response.mostRecentCommit);

        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        if (response.inProgressCommit.isAfter(inProgressCommit))
            inProgressCommit = response.inProgressCommit;

        return true;
    }

    protected PrepareSummary getResult()
    {
        Iterable<InetAddress> missingMRC = Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot));
            }
        });
        return PrepareSummary.successfulPrepare(mostRecentCommit, inProgressCommit, missingMRC);
    }
}
