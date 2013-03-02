package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public Commit mostRecentCommit = Commit.emptyCommit();
    public UUID inProgressBallot = UUIDGen.minTimeUUID(0);
    public Row inProgressUpdates = null;

    private Map<InetAddress, Commit> commitsByReplica = new HashMap<InetAddress, Commit>();
    private SortedMap<UUID, AtomicInteger> sharedCommits = new TreeMap<UUID, AtomicInteger>(FBUtilities.timeComparator);

    public PrepareCallback(int targets)
    {
        super(targets);
    }

    public synchronized void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);

        promised &= response.promised;
        commitsByReplica.put(message.from, response.mostRecentCommit);

        AtomicInteger mrc = sharedCommits.get(response.mostRecentCommit.ballot);
        if (mrc == null)
        {
            mrc = new AtomicInteger(0);
            sharedCommits.put(response.mostRecentCommit.ballot, mrc);
        }
        mrc.incrementAndGet();

        if (response.mostRecentCommit.ballot.timestamp() > mostRecentCommit.ballot.timestamp())
            mostRecentCommit = response.mostRecentCommit;

        if (response.inProgressBallot.timestamp() > inProgressBallot.timestamp())
        {
            inProgressBallot = response.inProgressBallot;
            inProgressUpdates = response.inProgressUpdates;
        }

        latch.countDown();
    }

    public boolean mostRecentCommitHasQuorum(int required)
    {
        return sharedCommits.get(sharedCommits.lastKey()).get() >= required;
    }

    public Iterable<InetAddress> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return commitsByReplica.get(inetAddress).ballot != mostRecentCommit.ballot;
            }
        });
    }
}
