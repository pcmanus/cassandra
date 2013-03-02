package org.apache.cassandra.service.paxos;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IAsyncCallback;

public abstract class AbstractPaxosCallback<T> implements IAsyncCallback<T>
{
    protected final CountDownLatch latch;
    protected final int targets;

    public AbstractPaxosCallback(int targets)
    {
        this.targets = targets;
        latch = new CountDownLatch(targets);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public int getResponseCount()
    {
        return (int) (targets - latch.getCount());
    }

    public void await()
    {
        try
        {
            latch.await(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }
    }
}
