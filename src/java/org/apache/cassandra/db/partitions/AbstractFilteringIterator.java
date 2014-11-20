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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;

/**
 * Abstract class to make it easier to write iterators that filter some
 * parts of another iterator (used for purging tombstones and removing dropped columns).
 */
// TODO rename to FilteringPartitionIterator for consistency
public abstract class AbstractFilteringIterator extends WrappingPartitionIterator
{
    protected final FilteringRow filter;

    private AtomIterator next;

    protected AbstractFilteringIterator(PartitionIterator iter, FilteringRow filter)
    {
        super(iter);
        this.filter = filter;
    }

    // Whether or not we should bother filtering the provided atom iterator. This
    // exists mainly for preformance
    protected boolean shouldFilter(AtomIterator iterator)
    {
        return true;
    }

    protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        return true;
    }

    protected boolean includePartitionDeletion(DeletionTime dt)
    {
        return true;
    }

    public boolean hasNext()
    {
        while (next == null && super.hasNext())
        {
            next = super.next();
            if (shouldFilter(next))
            {
                next = new FilteringIterator(next, filter);
                if (AtomIterators.isEmpty(next))
                    next = null;
            }

            if (next != null)
                return true;
        }
        return false;
    }

    public AtomIterator next()
    {
        AtomIterator toReturn = next;
        next = null;
        return toReturn;
    }

    private class FilteringIterator extends RowFilteringAtomIterator
    {
        private FilteringIterator(AtomIterator iterator, FilteringRow filter)
        {
            super(iterator, filter);
        }

        @Override
        protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            return AbstractFilteringIterator.this.includeRangeTombstoneMarker(marker);
        }

        @Override
        protected boolean includePartitionDeletion(DeletionTime dt)
        {
            return AbstractFilteringIterator.this.includePartitionDeletion(dt);
        }
    }
}
