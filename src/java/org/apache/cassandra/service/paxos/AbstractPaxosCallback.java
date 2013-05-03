package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.AbstractRequestCallback;

public abstract class AbstractPaxosCallback<M, V> extends AbstractRequestCallback<M, V>
{
    public AbstractPaxosCallback(int targets)
    {
        super(targets);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public WriteTimeoutException reportTimeout()
    {
        return new WriteTimeoutException(WriteType.CAS, ConsistencyLevel.SERIAL, getResponsesCount(), waitFor());
    }
}
