package org.apache.cassandra.service.paxos;

import java.util.UUID;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.UUIDGen;

public class Commit
{
    public final UUID ballot;
    public final ColumnFamily update;

    public Commit(UUID ballot, ColumnFamily update)
    {
        assert ballot != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit emptyCommit()
    {
        return new Commit(UUIDGen.minTimeUUID(0), null);
    }
}
