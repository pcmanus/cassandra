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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Serializer serializer = new Serializer();

    private static final Comparator<UnfilteredRowIterator> partitionComparator = (p1, p2) -> p1.partitionKey().compareTo(p2.partitionKey());

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

    public static class SingletonPartitionIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final UnfilteredRowIterator iterator;
        private boolean returned;
        private boolean isForThrift;

        public SingletonPartitionIterator(UnfilteredRowIterator iterator, boolean isForThrift)
        {
            this.iterator = iterator;
            this.isForThrift = isForThrift;
        }

        protected UnfilteredRowIterator computeNext()
        {
            if (returned)
                return endOfData();

            returned = true;
            return iterator;
        }

        public void close()
        {
            iterator.close();
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }
    }

    /**
     * Serialize each UnfilteredSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public void serialize(UnfilteredPartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                while (iter.hasNext())
                {
                    try (UnfilteredRowIterator partition = iter.next())
                    {
                        serializeLegacyPartition(partition, out, version);
                    }
                }
                return;
            }

            out.writeBoolean(iter.isForThrift());
            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(partition, out, version);
                }
            }
            out.writeBoolean(false);
        }

        public UnfilteredPartitionIterator deserialize(final DataInputPlus in, final int version, final SerializationHelper.Flag flag) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                int partitionCount = in.readInt();
                ArrayList<UnfilteredRowIterator> partitions = new ArrayList<>(partitionCount);
                for (int i = 0; i < partitionCount; i++)
                {
                    DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
                    boolean present = in.readBoolean();
                    assert present;
                    partitions.add(deserializeLegacyPartition(in, key, version));
                }
                final Iterator<UnfilteredRowIterator> iterator = partitions.iterator();

                return new AbstractUnfilteredPartitionIterator()
                {
                    UnfilteredRowIterator next = null;

                    public boolean isForThrift()
                    {
                        return true;
                    }

                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    public UnfilteredRowIterator next()
                    {
                        next = iterator.next();
                        return next;
                    }

                    @Override
                    public void close()
                    {
                        if (next != null)
                            next.close();
                    }
                };
            }

            final boolean isForThrift = in.readBoolean();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public boolean isForThrift()
                {
                    return isForThrift;
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
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
                        next = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, flag);
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

        void serializeLegacyPartition(UnfilteredRowIterator partition, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
            Pair<DeletionInfo, Pair<LegacyLayout.LegacyRangeTombstoneList, Iterator<LegacyLayout.LegacyCell>>> pair = LegacyLayout.fromUnfilteredRowIterator(partition);
            DeletionInfo deletionInfo = pair.left;
            LegacyLayout.LegacyRangeTombstoneList rtl = pair.right.left;

            // Processing the cell iterator results in the LegacyRangeTombstoneList being populated, so we do this
            // before we use the LegacyRangeTombstoneList at all
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right.right);

            out.writeBoolean(true);  // isPresent
            UUIDSerializer.serializer.serialize(partition.metadata().cfId, out, version);
            DeletionTime.serializer.serialize(deletionInfo.getPartitionDeletion(), out);

            // The LegacyRangeTombstoneList already has range tombstones for the single-row deletions and complex
            // deletions.  Go through our normal range tombstones and add then to the LegacyRTL so that the range
            // tombstones all get merged and sorted properly.
            if (deletionInfo.hasRanges())
            {
                Iterator<RangeTombstone> rangeTombstoneIterator = deletionInfo.rangeIterator(false);
                while (rangeTombstoneIterator.hasNext())
                {
                    RangeTombstone rt = rangeTombstoneIterator.next();
                    Slice slice = rt.deletedSlice();
                    LegacyLayout.LegacyBound start = new LegacyLayout.LegacyBound(slice.start(), false, null);
                    LegacyLayout.LegacyBound end = new LegacyLayout.LegacyBound(slice.end(), false, null);
                    rtl.add(start, end, rt.deletionTime().markedForDeleteAt(), rt.deletionTime().localDeletionTime());
                }
            }

            rtl.serialize(out, partition.metadata());

            // begin cell serialization
            out.writeInt(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                ByteBufferUtil.writeWithShortLength(cell.name.encode(partition.metadata()), out);
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    out.writeByte(LegacyLayout.EXPIRATION_MASK);  // serialization flags
                    out.writeInt(cell.ttl);
                    out.writeInt(cell.localDeletionTime);
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
                {
                    out.writeByte(LegacyLayout.DELETION_MASK);  // serialization flags
                    out.writeLong(cell.timestamp);
                    out.writeInt(TypeSizes.sizeof(cell.localDeletionTime));
                    out.writeInt(cell.localDeletionTime);
                    continue;
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    out.writeByte(LegacyLayout.COUNTER_MASK);  // serialization flags
                    out.writeLong(Long.MIN_VALUE);  // timestampOfLastDelete (not used, and MIN_VALUE is the default)
                }
                else
                {
                    // normal cell
                    out.writeByte(0);  // serialization flags
                }

                out.writeLong(cell.timestamp);
                ByteBufferUtil.writeWithLength(cell.value, out);
            }
        }

        public UnfilteredRowIterator deserializeLegacyPartition(DataInputPlus in, DecoratedKey key, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            LegacyLayout.LegacyDeletionInfo info = LegacyLayout.LegacyDeletionInfo.deserialize(metadata, in);
            int size = in.readInt();
            SerializationHelper helper = new SerializationHelper(metadata, version, SerializationHelper.Flag.FROM_REMOTE, ColumnFilter.all(metadata));
            Iterator<LegacyLayout.LegacyCell> cells = LegacyLayout.deserializeCells(metadata, in, SerializationHelper.Flag.FROM_REMOTE, size);
            return LegacyLayout.onWireCellstoUnfilteredRowIterator(metadata, key, info, cells, false, helper);
        }

        public long serializedSize(UnfilteredPartitionIterator iter, int version)
        {
            assert version < MessagingService.VERSION_30;

            if (!iter.hasNext())
                return TypeSizes.sizeof(false);

            long size = TypeSizes.sizeof(true);
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    size += serializedPartitionSize(partition, version);
                }
            }
            return size;
        }

        long serializedPartitionSize(UnfilteredRowIterator partition, int version)
        {
            assert version < MessagingService.VERSION_30;
            long size = ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
            Pair<DeletionInfo, Pair<LegacyLayout.LegacyRangeTombstoneList, Iterator<LegacyLayout.LegacyCell>>> pair = LegacyLayout.fromUnfilteredRowIterator(partition);
            DeletionInfo deletionInfo = pair.left;
            LegacyLayout.LegacyRangeTombstoneList rtl = pair.right.left;

            // Processing the cell iterator results in the LegacyRangeTombstoneList being populated, so we do this
            // before we use the LegacyRangeTombstoneList at all
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right.right);

            size += TypeSizes.sizeof(true);
            size += UUIDSerializer.serializer.serializedSize(partition.metadata().cfId, version);
            size += DeletionTime.serializer.serializedSize(pair.left.getPartitionDeletion());

            if (deletionInfo.hasRanges())
            {
                Iterator<RangeTombstone> rangeTombstoneIterator = deletionInfo.rangeIterator(false);
                while (rangeTombstoneIterator.hasNext())
                {
                    RangeTombstone rt = rangeTombstoneIterator.next();
                    Slice slice = rt.deletedSlice();
                    LegacyLayout.LegacyBound start = new LegacyLayout.LegacyBound(slice.start(), false, null);
                    LegacyLayout.LegacyBound end = new LegacyLayout.LegacyBound(slice.end(), false, null);
                    rtl.add(start, end, rt.deletionTime().markedForDeleteAt(), rt.deletionTime().localDeletionTime());
                }
            }

            size += rtl.serializedSize(partition.metadata());

            // begin cell serialization
            size += TypeSizes.sizeof(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(cell.name.encode(partition.metadata()));
                size += 1;  // serialization flags
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    size += TypeSizes.sizeof(cell.ttl);
                    size += TypeSizes.sizeof(cell.localDeletionTime);
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
                {
                    size += TypeSizes.sizeof(cell.timestamp);
                    // localDeletionTime replaces cell.value as the body
                    size += TypeSizes.sizeof(TypeSizes.sizeof(cell.localDeletionTime));
                    size += TypeSizes.sizeof(cell.localDeletionTime);
                    continue;
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    size += TypeSizes.sizeof(Long.MIN_VALUE);  // timestampOfLastDelete
                }

                size += TypeSizes.sizeof(cell.timestamp);
                size += ByteBufferUtil.serializedSizeWithLength(cell.value);
            }

            return size;
        }
    }
}