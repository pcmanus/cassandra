package org.apache.cassandra.service.paxos;

import java.net.InetAddress;

public class PrepareSummary
{
    public final boolean promised;
    public final Commit mostRecentCommit;
    public final Commit inProgressCommit;
    public final Iterable<InetAddress> replicasMissingMostRecentCommit;

    private PrepareSummary(boolean promised, Commit mostRecentCommit, Commit inProgressCommit, Iterable<InetAddress> missingMRC)
    {
        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
        this.replicasMissingMostRecentCommit = missingMRC;
    }

    public static PrepareSummary failedPrepare()
    {
        return new PrepareSummary(false, null, null, null);
    }

    public static PrepareSummary successfulPrepare(Commit mostRecentCommit, Commit inProgressCommit, Iterable<InetAddress> missingMRC)
    {
        return new PrepareSummary(true, mostRecentCommit, inProgressCommit, missingMRC);
    }
}
