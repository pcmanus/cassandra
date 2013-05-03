package org.apache.cassandra.service.paxos;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.net.MessageIn;

public class ProposeCallback extends AbstractPaxosCallback<Boolean, Boolean>
{
    private final AtomicInteger successful = new AtomicInteger(0);

    public ProposeCallback(int targets)
    {
        super(targets);
    }

    protected boolean process(MessageIn<Boolean> message)
    {
        if (message.payload)
            successful.incrementAndGet();
        return true;
    }

    public Boolean getResult()
    {
        return successful.get() >= waitFor();
    }
}
