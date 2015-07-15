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
package org.apache.cassandra.db.partitions;

import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MergeIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredPartitionIterators.class);

    private static final Serializer serializer = new Serializer();

    private static final Comparator<UnfilteredRowIterator> partitionComparator = new Comparator<UnfilteredRowIterator>()
    {
        public int compare(UnfilteredRowIterator p1, UnfilteredRowIterator p2)
        {
            return p1.partitionKey().compareTo(p2.partitionKey());
        }
    };

    public static final UnfilteredPartitionIterator EMPTY = new AbstractUnfilteredPartitionIterator()
    {
        public boolean isForThrift()
        {
            return false;
        }

        public boolean hasNext()
        {
            return false;
        }

        public UnfilteredRowIterator next()
        {
            throw new NoSuchElementException();
        }
    };

    private UnfilteredPartitionIterators() {}

    public interface MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions);
        public void close();
    }

    @SuppressWarnings("resource") // The created resources are returned right away
    public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        UnfilteredRowIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : UnfilteredRowIterators.emptyIterator(command.metadata(),
                                                                     command.partitionKey(),
                                                                     command.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole UnfilteredPartitionIterator.
        return new WrappingUnfilteredRowIterator(toReturn)
        {
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    // asserting this only now because it bothers Serializer if hasNext() is called before
                    // the previously returned iterator hasn't been fully consumed.
                    assert !iter.hasNext();

                    iter.close();
                }
            }
        };
    }

    public static PartitionIterator mergeAndFilter(List<UnfilteredPartitionIterator> iterators, int nowInSec, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the UnfilteredRowIterators directly as RowIterators
        return filter(merge(iterators, nowInSec, listener), nowInSec);
    }

    public static PartitionIterator filter(final UnfilteredPartitionIterator iterator, final int nowInSec)
    {
        return new PartitionIterator()
        {
            private RowIterator next;

            public boolean hasNext()
            {
                while (next == null && iterator.hasNext())
                {
                    @SuppressWarnings("resource") // closed either directly if empty, or, if assigned to next, by either
                                                  // the caller of next() or close()
                    UnfilteredRowIterator rowIterator = iterator.next();
                    next = UnfilteredRowIterators.filter(rowIterator, nowInSec);
                    if (!iterator.isForThrift() && next.isEmpty())
                    {
                        rowIterator.close();
                        next = null;
                    }
                }
                return next != null;
            }

            public RowIterator next()
            {
                if (next == null && !hasNext())
                    throw new NoSuchElementException();

                RowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                try
                {
                    iterator.close();
                }
                finally
                {
                    if (next != null)
                        next.close();
                }
            }
        };
    }

    public static UnfilteredPartitionIterator merge(final List<? extends UnfilteredPartitionIterator> iterators, final int nowInSec, final MergeListener listener)
    {
        assert listener != null;
        assert !iterators.isEmpty();

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            private CFMetaData metadata;
            private DecoratedKey partitionKey;
            private boolean isReverseOrder;

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                metadata = current.metadata();
                partitionKey = current.partitionKey();
                isReverseOrder = current.isReverseOrder();

                // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                // Non-present iterator will thus be set to empty in getReduced.
                toMerge.set(idx, current);
            }

            protected UnfilteredRowIterator getReduced()
            {
                UnfilteredRowIterators.MergeListener rowListener = listener.getRowMergeListener(partitionKey, toMerge);

                // Replace nulls by empty iterators
                for (int i = 0; i < toMerge.size(); i++)
                    if (toMerge.get(i) == null)
                        toMerge.set(i, UnfilteredRowIterators.emptyIterator(metadata, partitionKey, isReverseOrder));

                return UnfilteredRowIterators.merge(toMerge, nowInSec, rowListener);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                for (int i = 0; i < iterators.size(); i++)
                    toMerge.add(null);
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static UnfilteredPartitionIterator mergeLazily(final List<? extends UnfilteredPartitionIterator> iterators, final int nowInSec)
    {
        assert !iterators.isEmpty();

        if (iterators.size() == 1)
            return iterators.get(0);

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return false;
            }

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                toMerge.add(current);
            }

            protected UnfilteredRowIterator getReduced()
            {
                return new LazilyInitializedUnfilteredRowIterator(toMerge.get(0).partitionKey())
                {
                    protected UnfilteredRowIterator initializeIterator()
                    {
                        return UnfilteredRowIterators.merge(toMerge, nowInSec);
                    }
                };
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static void digest(UnfilteredPartitionIterator iterator, MessageDigest digest)
    {
        try (UnfilteredPartitionIterator iter = iterator)
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIterators.digest(partition, digest);
                }
            }
        }
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    /**
     * Wraps the provided iterator so it logs the returned rows/RT for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails)
    {
        return new WrappingUnfilteredPartitionIterator(iterator)
        {
            public UnfilteredRowIterator next()
            {
                return UnfilteredRowIterators.loggingIterator(super.next(), id, fullDetails);
            }
        };
    }

    /**
     * Writes the serialization header first, and then each partition in the iterator one after the other.
     */
    public static class Serializer
    {
        public void serialize(UnfilteredPartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            if (!iter.hasNext())
            {
                SerializationHeader.serializer.writeEmptyMessage(out);
                return;
            }

            SerializationHeader header = new SerializationHeader(iter.metadata(), iter.columns(), iter.stats(), iter.isForThrift());
            SerializationHeader.serializer.serializeForMessaging(header, out, version);

            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(partition, out, version, header);
                }
            }
            UnfilteredRowIteratorSerializer.serializer.writeEndOfMessage(out);
        }

        public UnfilteredPartitionIterator deserialize(final CFMetaData metadata, final DataInputPlus in, final int version, final SerializationHelper.Flag flag, final boolean isForThrift) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            final SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(metadata, in, version);
            if (header == null)
                return empty();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean nextReturned = true;

                public boolean isForThrift()
                {
                    return header.isForThrift();
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return next != null;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        next = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, flag, header);
                        nextReturned = false;
                        return next != null;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public UnfilteredRowIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        return next;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }
    }
}
