/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

public class CounterMutation implements IMutation
{
    public static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentCounterWriters() * 1024);

    private final Mutation mutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(Mutation mutation, ConsistencyLevel consistency)
    {
        this.mutation = mutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return mutation.getColumnFamilyIds();
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public DecoratedKey key()
    {
        return mutation.key();
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public MessageOut<CounterMutation> makeMutationMessage()
    {
        return new MessageOut<>(MessagingService.Verb.COUNTER_MUTATION, this, serializer);
    }

    /**
     * Applies the counter mutation, returns the result Mutation (for replication to other nodes).
     *
     * 1. Grabs the striped cell-level locks in the proper order
     * 2. Gets the current values of the counters-to-be-modified from the counter cache
     * 3. Reads the rest of the current values (cache misses) from the CF
     * 4. Writes the updated counter values
     * 5. Updates the counter cache
     * 6. Releases the lock(s)
     *
     * See CASSANDRA-4775 and CASSANDRA-6504 for further details.
     *
     * @return the applied resulting Mutation
     */
    public Mutation apply() throws WriteTimeoutException
    {
        Mutation result = new Mutation(getKeyspaceName(), key());
        Keyspace keyspace = Keyspace.open(getKeyspaceName());

        List<Lock> locks = new ArrayList<>();
        Tracing.trace("Acquiring counter locks");
        try
        {
            grabCounterLocks(keyspace, locks);
            for (PartitionUpdate upd : getPartitionUpdates())
                result.add(processModifications(upd));
            result.apply();
            return result;
        }
        finally
        {
            for (Lock lock : locks)
                lock.unlock();
        }
    }

    private void grabCounterLocks(Keyspace keyspace, List<Lock> locks) throws WriteTimeoutException
    {
        long startTime = System.nanoTime();

        for (Lock lock : LOCKS.bulkGet(getCounterLockKeys()))
        {
            long timeout = TimeUnit.MILLISECONDS.toNanos(getTimeout()) - (System.nanoTime() - startTime);
            try
            {
                if (!lock.tryLock(timeout, TimeUnit.NANOSECONDS))
                    throw new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace));
                locks.add(lock);
            }
            catch (InterruptedException e)
            {
                throw new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace));
            }
        }
    }

    /**
     * Returns a wrapper for the Striped#bulkGet() call (via Keyspace#counterLocksFor())
     * Striped#bulkGet() depends on Object#hashCode(), so here we make sure that the cf id and the partition key
     * all get to be part of the hashCode() calculation, not just the cell name.
     */
    private Iterable<Object> getCounterLockKeys()
    {
        return Iterables.concat(Iterables.transform(getPartitionUpdates(), new Function<PartitionUpdate, Iterable<Object>>()
        {
            public Iterable<Object> apply(final PartitionUpdate update)
            {
                return Iterables.concat(Iterables.transform(update, new Function<RowUpdate, Iterable<Object>>()
                {
                    public Iterable<Object> apply(final RowUpdate row)
                    {
                        return Iterables.concat(Iterables.transform(row, new Function<ColumnData, Object>()
                        {
                            public Object apply(final ColumnData data)
                            {
                                return Objects.hashCode(update.metadata().cfId, key(), row.clustering(), data.column());
                            }
                        }));
                    }
                }));
            }
        }));
    }

    // Replaces all the CounterUpdateCell-s with updated regular CounterCell-s
    private PartitionUpdate processModifications(PartitionUpdate changes)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changes.metadata().cfId);

        PartitionUpdate resultUpdate = new PartitionUpdate(changes.metadata(), changes.partitionKey());
        resultUpdate.deletionInfo().add(changes.deletionInfo());

        Map<ClusteringPrefix, Map<ColumnDefinition, CounterUpdate>> counterUpdates = new HashMap<>();

        for (RowUpdate upd : changes)
        {
            ClusteringPrefix clustering = upd.clustering();
            RowUpdate newUpd = RowUpdates.create(clustering, upd.columns());
            resultUpdate.add(newUpd);
            newUpd.updateRowTimestamp(upd.timestamp());
            for (ColumnData data : upd)
            {
                Cell cell = data.cell(0);
                if (cell.isCounterCell())
                {
                    newUpd.addCell(data.column(), cell);
                }
                else
                {
                    CounterUpdate c = new CounterUpdate(cell.value(), cell.timestamp());
                    newUpd.addCell(data.column(), c);

                    Map<ColumnDefinition, CounterUpdate> m = counterUpdates.get(clustering);
                    if (m == null)
                    {
                        m = new HashMap<>();
                        counterUpdates.put(clustering, m);
                    }
                    m.put(data.column(), c);
                }
            }
        }
        updateWithCurrentValues(counterUpdates, cfs);
        return resultUpdate;
    }

    private void updateWithCurrentValue(ClusteringPrefix clustering, ColumnDefinition column, CounterUpdate toUpdate, ClockAndCount currentValue, ColumnFamilyStore cfs)
    {
        long clock = currentValue.clock + 1L;
        long count = currentValue.count + ByteBufferUtil.toLong(toUpdate.value());
        toUpdate.setValue(CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count));

        // Cache the newly updated value
        cfs.putCachedCounter(key().getKey(), clustering, column, ClockAndCount.create(clock, count));
    }

    // Attempt to load the current values from cache. If that fails, read the rest from the cfs.
    private void updateWithCurrentValues(Map<ColumnPath, CounterUpdate> counterUpdates, ColumnFamilyStore cfs)
    {
        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", counterUpdates.size());
            updateWithCurrentValuesFromCache(counterUpdates, cfs);
            if (counterUpdates.isEmpty())
                return;
        }

        Tracing.trace("Reading {} counter values from the CF", counterUpdates.size());
        updateWithCurrentValuesFromCFS(counterUpdates, cfs);

        // What's remain is new counters
        for (Map.Entry<ClusteringPrefix, Map<ColumnDefinition, CounterUpdate>> entry : counterUpdates.entrySet())
        {
            ClusteringPrefix clustering = entry.getKey();
            for (Map.Entry<ColumnDefinition, CounterUpdate> subEntry : entry.getValue().entrySet())
                updateWithCurrentValue(clustering, subEntry.getKey(), subEntry.getValue(), ClockAndCount.BLANK, cfs);
        }
    }

    // Returns the count of cache misses.
    private void updateWithCurrentValuesFromCache(Map<ColumnPath, CounterUpdate> counterUpdates, ColumnFamilyStore cfs)
    {
        Iterator<Map.Entry<ClusteringPrefix, Map<ColumnDefinition, CounterUpdate>>> iter = counterUpdates.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<ClusteringPrefix, Map<ColumnDefinition, CounterUpdate>> entry = iter.next();
            ClusteringPrefix clustering = entry.getKey();
            Iterator<Map.Entry<ColumnDefinition, CounterUpdate>> subIter = entry.getValue().entrySet().iterator();
            while (subIter.hasNext())
            {
                Map.Entry<ColumnDefinition, CounterUpdate> subEntry = subIter.next();
                ColumnDefinition column = subEntry.getKey();
                CounterUpdate upd = subEntry.getValue();
                ClockAndCount cached = cfs.getCachedCounter(key().getKey(), clustering, column);
                if (cached != null)
                {
                    updateWithCurrentValue(clustering, column, upd, cached, cfs);
                    subIter.remove();
                }
            }
            if (entry.getValue().isEmpty())
                iter.remove();
        }
    }

    // Reads the missing current values from the CFS.
    private void updateWithCurrentValuesFromCFS(Map<ColumnPath, CounterUpdate> counterUpdates, ColumnFamilyStore cfs)
    {
        Columns.Builder builder = Columns.builder();
        SortedSet<ClusteringPrefix> names = new TreeSet<>(cfs.metadata.comparator);
        for (Map.Entry<ClusteringPrefix, Map<ColumnDefinition, CounterUpdate>> entry : counterUpdates.entrySet())
        {
            names.add(entry.getKey());
            for (ColumnDefinition column : entry.getValue().keySet())
                builder.add(column);
        }

        int nowInSec = FBUtilities.nowInSeconds();
        NamesPartitionFilter filter = new NamesPartitionFilter(builder.regularColumns(), builder.staticColumns(), names);
        SinglePartitionReadCommand cmd = ReadCommands.create(cfs.metadata, key(), filter, DataLimits.NONE, nowInSec);
        try (AtomIterator partition = cmd.queryMemtableAndDisk(cfs))
        {
            RowIterator rowIter = AtomIterators.asRowIterator(partition, nowInSec);
            while (rowIter.hasNext())
            {
                Row row = rowIter.next();
                ClusteringPrefix clustering = row.clustering();
                Map<ColumnDefinition, CounterUpdate> m = counterUpdates.get(clustering);
                if (m == null || m.isEmpty())
                    continue;

                Iterator<Map.Entry<ColumnDefinition, CounterUpdate>> subIter = m.entrySet().iterator();
                while (subIter.hasNext())
                {
                    Map.Entry<ColumnDefinition, CounterUpdate> entry = subIter.next();
                    Cell existing = Rows.getCell(row, entry.getKey());
                    if (existing != null)
                    {
                        updateWithCurrentValue(clustering, entry.getKey(), entry.getValue(), CounterContext.instance().getLocalClockAndCount(existing.value()), cfs);
                        subIter.remove();
                    }
                }
                if (m.isEmpty())
                    counterUpdates.remove(row.clustering());
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void addAll(IMutation m)
    {
        if (!(m instanceof CounterMutation))
            throw new IllegalArgumentException();
        CounterMutation cm = (CounterMutation)m;
        mutation.addAll(cm.mutation);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        return String.format("CounterMutation(%s, %s)", mutation.toString(shallow), consistency);
    }

    private static class CounterUpdate implements Cell
    {
        private ByteBuffer value;
        private final long timestamp;

        public CounterUpdate(ByteBuffer initialValue, long timestamp)
        {
            this.value = initialValue;
            this.timestamp = timestamp;
        }

        public void setValue(ByteBuffer value)
        {
            this.value = value;
        }

        public boolean isCounterCell()
        {
            return true;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public long timestamp()
        {
            return timestamp;
        }

        public int localDeletionTime()
        {
            return Integer.MAX_VALUE;
        }

        public int ttl()
        {
            return 0;
        }

        public CellPath path()
        {
            return null;
        }

        public Cell takeAlias()
        {
            return this;
        }
    }

    public static class CounterMutationSerializer implements IVersionedSerializer<CounterMutation>
    {
        public void serialize(CounterMutation cm, DataOutputPlus out, int version) throws IOException
        {
            Mutation.serializer.serialize(cm.mutation, out, version);
            out.writeUTF(cm.consistency.name());
        }

        public CounterMutation deserialize(DataInput in, int version) throws IOException
        {
            Mutation m = Mutation.serializer.deserialize(in, version);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm, int version)
        {
            return Mutation.serializer.serializedSize(cm.mutation, version)
                 + TypeSizes.NATIVE.sizeof(cm.consistency.name());
        }
    }
}
