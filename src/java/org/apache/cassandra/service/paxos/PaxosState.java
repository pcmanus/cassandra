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

    private final Commit inProgressCommit;
    private final Commit mostRecentCommit;

    public PaxosState(ByteBuffer key)
    {
        this(Commit.emptyCommit(key), Commit.emptyCommit(key));
    }

    public PaxosState(Commit inProgressCommit, Commit mostRecentCommit)
    {
        this.inProgressCommit = inProgressCommit;
        this.mostRecentCommit = mostRecentCommit;
    }

    public static PrepareResponse prepare(Commit toPrepare)
    {
        synchronized (lockFor(toPrepare.key))
        {
            PaxosState state = SystemTable.loadPaxosState(toPrepare.key);
            if (toPrepare.isAfter(state.inProgressCommit))
            {
                logger.debug("promising ballot {}", toPrepare.ballot);
                SystemTable.savePaxosPromise(toPrepare);
                // return the pre-promise ballot so coordinator can pick the most recent in-progress value to resume
                return new PrepareResponse(true, state.inProgressCommit, state.mostRecentCommit);
            }
            else
            {
                logger.debug("promise rejected; {} is not sufficiently newer than {}", toPrepare, state.inProgressCommit);
                return new PrepareResponse(false, state.inProgressCommit, state.mostRecentCommit);
            }
        }
    }

    public static Boolean propose(Commit proposal)
    {
        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemTable.loadPaxosState(proposal.key);
            if (proposal.hasBallot(state.inProgressCommit.ballot))
            {
                logger.debug("accepting {}", proposal);
                SystemTable.savePaxosProposal(proposal);
                return true;
            }

            logger.debug("accept requested for {} but inProgress is now {}", proposal, state.inProgressCommit);
            return false;
        }
    }

    public static void commit(Commit proposal)
    {
        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemTable.loadPaxosState(proposal.key);
            if (proposal.hasBallot(state.inProgressCommit.ballot))
            {
                logger.debug("committing {}", proposal);

                RowMutation rm = proposal.makeMutation();
                Table.open(rm.getTable()).apply(rm, true);
                SystemTable.savePaxosCommit(proposal);
            }
            else
            {
                // a new coordinator extracted a promise from us before the old one issued its commit.
                // (this means the new one should also issue a commit soon.)
                logger.debug("commit requested for {} but inProgress is now {}", proposal, state.inProgressCommit);
            }
        }
    }
}
