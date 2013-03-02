package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PaxosState
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosState.class);

    private static final int STATE_BUCKETS = 1024;

    private static final Map<Integer, PaxosState> states;

    static
    {
        ImmutableMap.Builder<Integer, PaxosState> builder = ImmutableMap.builder();
        for (int i = 0; i < STATE_BUCKETS; i++)
            builder.put(i, SystemTable.loadPaxosState(i));
        states = builder.build();
    }

    public static PaxosState stateFor(ByteBuffer key)
    {
        return states.get(key.hashCode() % STATE_BUCKETS);
    }

    private int id;
    private UUID inProgressBallot;
    private Row acceptedProposal;
    private Commit mostRecentCommit;


    public PaxosState(int id)
    {
        this(id, UUIDGen.minTimeUUID(0), null, Commit.emptyCommit());
    }

    public PaxosState(int id, UUID inProgressBallot, Row acceptedProposal, Commit mostRecentCommit)
    {
        this.id = id;
        this.inProgressBallot = inProgressBallot;
        this.acceptedProposal = acceptedProposal;
        this.mostRecentCommit = mostRecentCommit;
    }

    /**
     * If writing to CommitLog, caller should synchronize with this to make sure that commitlog replay
     * order matches the order we apply live.
     */
    public synchronized PrepareResponse prepare(UUID ballot)
    {
        if (FBUtilities.timeComparator.compare(ballot, inProgressBallot) > 0)
        {
            logger.debug("promising ballot {}", ballot);
            try
            {
                // return the pre-promise ballot so coordinator can pick the most recent in-progress value to resume
                return new PrepareResponse(true, inProgressBallot, acceptedProposal, mostRecentCommit);
            }
            finally
            {
                inProgressBallot = ballot;
                SystemTable.savePaxosPromise(id, ballot);
            }
        }
        else
        {
            logger.debug("promise rejected; {} is not sufficiently newer than {}", ballot, inProgressBallot);
            return new PrepareResponse(false, inProgressBallot, acceptedProposal, mostRecentCommit);
        }
    }

    /**
     * If writing to CommitLog, caller should synchronize with this to make sure that commitlog replay
     * order matches the order we apply live.
     */
    public synchronized Boolean propose(UUID ballot, Row proposal)
    {
        if (inProgressBallot.equals(ballot))
        {
            logger.debug("accepting {} for {}", ballot, proposal);
            acceptedProposal = proposal;
            SystemTable.savePaxosProposal(id, ballot, proposal);
            return true;
        }

        logger.debug("accept requested for {} but inProgressBallot is now {}", ballot, inProgressBallot);
        return false;
    }

    /**
     * Caller does not need to update the commitlog; commit will log a RowMutation
     */
    public synchronized void commit(UUID ballot, Row proposal)
    {
        if (inProgressBallot.equals(ballot))
        {
            logger.debug("committing {} for {}", proposal, ballot);

            RowMutation rm = new RowMutation(proposal.key.key, proposal.cf);
            Table.open(rm.getTable()).apply(rm, true);
            mostRecentCommit = new Commit(ballot, proposal);
            acceptedProposal = null;
            SystemTable.savePaxosCommit(id, ballot, proposal);
        }
        else
        {
            // a new coordinator extracted a promise from us before the old one issued its commit.
            // (this means the new one should also issue a commit soon.)
            logger.debug("commit requested for {} but inProgressBallot is now {}", ballot, inProgressBallot);
        }
    }
}
