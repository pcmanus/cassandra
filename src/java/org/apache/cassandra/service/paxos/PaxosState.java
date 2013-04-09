package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PaxosState
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosState.class);

    private static final Object[] locks;
    static
    {
        locks = new Object[1024];
        for (int i = 0; i < locks.length; i++)
            locks[i] = new Object();
    }
    private static Object lockFor(ByteBuffer key)
    {
        return locks[key.hashCode() % locks.length];
    }

    private final UUID inProgressBallot;
    private final ColumnFamily acceptedProposal;
    private final Commit mostRecentCommit;

    public PaxosState()
    {
        this(UUIDGen.minTimeUUID(0), null, Commit.emptyCommit());
    }

    public PaxosState(UUID inProgressBallot, ColumnFamily acceptedProposal, Commit mostRecentCommit)
    {
        this.inProgressBallot = inProgressBallot;
        this.acceptedProposal = acceptedProposal;
        this.mostRecentCommit = mostRecentCommit;
    }

    public static PrepareResponse prepare(ByteBuffer key, UUID ballot)
    {
        synchronized (lockFor(key))
        {
            PaxosState state = SystemTable.loadPaxosState(key);
            if (FBUtilities.timeComparator.compare(ballot, state.inProgressBallot) > 0)
            {
                logger.debug("promising ballot {}", ballot);
                SystemTable.savePaxosPromise(key, ballot);
                // return the pre-promise ballot so coordinator can pick the most recent in-progress value to resume
                return new PrepareResponse(true, state.inProgressBallot, state.acceptedProposal, state.mostRecentCommit);
            }
            else
            {
                logger.debug("promise rejected; {} is not sufficiently newer than {}", ballot, state.inProgressBallot);
                return new PrepareResponse(false, state.inProgressBallot, state.acceptedProposal, state.mostRecentCommit);
            }
        }
    }

    public static Boolean propose(ByteBuffer key, UUID ballot, ColumnFamily proposal)
    {
        synchronized (lockFor(key))
        {
            PaxosState state = SystemTable.loadPaxosState(key);
            if (state.inProgressBallot.equals(ballot))
            {
                logger.debug("accepting {} for {}", ballot, proposal);
                SystemTable.savePaxosProposal(key, ballot, proposal);
                return true;
            }

            logger.debug("accept requested for {} but inProgressBallot is now {}", ballot, state.inProgressBallot);
            return false;
        }
    }

    public static void commit(ByteBuffer key, UUID ballot, ColumnFamily proposal)
    {
        synchronized (lockFor(key))
        {
            PaxosState state = SystemTable.loadPaxosState(key);
            if (state.inProgressBallot.equals(ballot))
            {
                logger.debug("committing {} for {}", proposal, ballot);

                RowMutation rm = new RowMutation(key, proposal);
                Table.open(rm.getTable()).apply(rm, true);
                SystemTable.savePaxosCommit(key, ballot, proposal);
            }
            else
            {
                // a new coordinator extracted a promise from us before the old one issued its commit.
                // (this means the new one should also issue a commit soon.)
                logger.debug("commit requested for {} but inProgressBallot is now {}", ballot, state.inProgressBallot);
            }
        }
    }
}
