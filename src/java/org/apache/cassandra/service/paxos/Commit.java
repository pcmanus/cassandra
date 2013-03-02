package org.apache.cassandra.service.paxos;

import java.util.UUID;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.utils.UUIDGen;

public class Commit
{
    public final UUID ballot;
    public final Row update;

    public Commit(UUID ballot, Row update)
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
