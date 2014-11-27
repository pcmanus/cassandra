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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Static methods to work with partition iterators.
 */
public abstract class PartitionIterators
{
    private static final Serializer serializer = new Serializer();

    public static final PartitionIterator EMPTY = new PartitionIterator()
    {
        public boolean hasNext()
        {
            return false;
        }

        public AtomIterator next()
        {
            throw new NoSuchElementException();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public void close()
        {
        }
    };

    private PartitionIterators() {}

    public interface MergeListener
    {
        public AtomIterators.MergeListener getAtomMergeListener(DecoratedKey partitionKey, AtomIterator[] versions);
        public void close();
    }

    public static DataIterator mergeAsDataIterator(List<PartitionIterator> iterators, int nowInSec, MergeListener listener)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static DataIterator asDataIterator(final PartitionIterator iterator, final int nowInSec)
    {
        return new DataIterator()
        {
            private RowIterator next;

            public boolean hasNext()
            {
                while (next == null && iterator.hasNext())
                {
                    next = new RowIteratorFromAtomIterator(iterator.next(), nowInSec);
                    if (RowIterators.isEmpty(next))
                        next = null;
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

            public void close() throws IOException
            {
                iterator.close();
            }
        };
    }

    public static PartitionIterator merge(List<? extends PartitionIterator> iterators, int nowInSec, MergeListener listener)
    {
        // TODO (make sure we special the case were there is only one iterator (even if there is a listener))
        throw new UnsupportedOperationException();
    }

    public static PartitionIterator merge(List<? extends PartitionIterator> iterators, int nowInSec)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        // TODO
        throw new UnsupportedOperationException();
    }

    public static PartitionIterator removeDroppedColumns(PartitionIterator iterator, final Map<ColumnIdentifier, Long> droppedColumns)
    {
        FilteringRow filter = new FilteringRow()
        {
            protected boolean includeCell(Cell cell)
            {
                return include(cell.column(), cell.timestamp());
            }

            protected boolean includeDeletion(ColumnDefinition c, DeletionTime dt)
            {
                return include(c, dt.markedForDeleteAt());
            }

            private boolean include(ColumnDefinition column, long timestamp)
            {
                Long droppedAt = droppedColumns.get(column.name);
                return droppedAt != null && timestamp <= droppedAt;
            }
        };

        return new AbstractFilteringIterator(iterator, filter)
        {
            protected boolean shouldFilter(AtomIterator atoms)
            {
                // TODO: We could have atom iterators return the smallest timestamp they might return
                // (which we can get from sstable stats), and ignore any dropping if that smallest
                // timestamp is bigger that the biggest droppedColumns timestamp.

                // If none of the dropped columns is part of the columns that the iterator actually returns, there is nothing to do;
                for (ColumnDefinition c : atoms.columns())
                    if (droppedColumns.containsKey(c.name))
                        return true;

                return false;
            }
        };
    }

    public static void digest(PartitionIterator iter, MessageDigest digest)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    public static class Serializer implements IVersionedSerializer<PartitionIterator>
    {
        public void serialize(PartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public PartitionIterator deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public PartitionIterator deserialize(DataInput in, int version, LegacyLayout.Flag flag) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public long serializedSize(PartitionIterator iter, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
        }
    }
}
