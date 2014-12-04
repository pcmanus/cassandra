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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Represents the selection of multiple range of rows within a partition.
 */
public abstract class Slices
{
    public static final Slices ALL = new SelectAllSlices();
    public static final Slices NONE = new SelectNoSlices();

    protected Slices()
    {
    }

    public static Slices make(ClusteringComparator cmp, ClusteringPrefix start, ClusteringPrefix end)
    {
        assert cmp.compare(start, end) <= 0;

        return start == EmptyClusteringPrefix.BOTTOM && end == EmptyClusteringPrefix.TOP
             ? Slices.ALL
             : new ArrayBackedSlices(cmp, new ClusteringPrefix[]{ start.takeAlias() }, new ClusteringPrefix[]{ end.takeAlias() });
    }

    public static Slices make(ClusteringComparator cmp, ClusteringPrefix prefix)
    {
        return new ArrayBackedSlices(cmp,
                                     new ClusteringPrefix[]{ prefix.withEOC(ClusteringPrefix.EOC.START).takeAlias() },
                                     new ClusteringPrefix[]{ prefix.withEOC(ClusteringPrefix.EOC.END).takeAlias() });
    }

    public abstract boolean hasLowerBound();
    public abstract ClusteringPrefix lowerBound();
    public abstract boolean hasUpperBound();
    public abstract ClusteringPrefix upperBound();

    public abstract ClusteringPrefix getStart(int i);
    public abstract ClusteringPrefix getEnd(int i);

    public abstract int size();

    public abstract Slices withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart);
    public abstract Slices withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd);

    public abstract InOrderTester inOrderTester(boolean reversed);
    public abstract boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues);
    public abstract AtomIterator makeSliceIterator(SeekableAtomIterator iter);

    public interface InOrderTester
    {
        public boolean intersects(ClusteringPrefix start, ClusteringPrefix end);
        public boolean includes(ClusteringPrefix value);
        public boolean isDone();
    }

    public static class Builder
    {
        private final ClusteringComparator comparator;

        private final ClusteringPrefix[] starts;
        private final ClusteringPrefix[] ends;
        private int current = -1;

        public Builder(ClusteringComparator comparator, int size)
        {
            this.comparator = comparator;
            this.starts = new ClusteringPrefix[size];
            this.ends = new ClusteringPrefix[size];
        }

        public Builder add(ClusteringPrefix start, ClusteringPrefix end)
        {
            assert comparator.compare(start, end) <= 0;
            starts[current] = start;
            ends[current] = end;
            current++;
            return this;
        }

        public Slices build()
        {
            ClusteringPrefix[] s = starts;
            ClusteringPrefix[] e = ends;
            if (current < starts.length)
            {
                s = Arrays.copyOf(starts, current);
                e = Arrays.copyOf(ends, current);
            }
            return new ArrayBackedSlices(comparator, s, e);
        }
    }

    private static class ArrayBackedSlices extends Slices
    {
        private final ClusteringComparator comparator;

        private final ClusteringPrefix[] starts;
        private final ClusteringPrefix[] ends;

        private ArrayBackedSlices(ClusteringComparator comparator, ClusteringPrefix[] starts, ClusteringPrefix[] ends)
        {
            assert starts.length == ends.length;
            this.comparator = comparator;
            this.starts = starts;
            this.ends = ends;
        }

        public int size()
        {
            return starts.length;
        }

        public boolean hasLowerBound()
        {
            return starts[0] != EmptyClusteringPrefix.BOTTOM;
        }

        public ClusteringPrefix lowerBound()
        {
            return starts[0];
        }

        public boolean hasUpperBound()
        {
            return ends[ends.length - 1] != EmptyClusteringPrefix.TOP;
        }

        public ClusteringPrefix upperBound()
        {
            return ends[ends.length - 1];
        }

        public ClusteringPrefix getStart(int i)
        {
            return starts[i];
        }

        public ClusteringPrefix getEnd(int i)
        {
            return ends[i];
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return reversed ? new InReverseOrderTester() : new InForwardOrderTester();
        }

        public Slices withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart)
        {
            for (int i = 0; i < starts.length; i++)
            {
                if (comparator.compare(ends[i], newStart) < 0)
                    continue;

                // newStart <= ends[i]. 
                int cmp = comparator.compare(newStart, starts[i]);
                if (i == 0 && cmp <= 0)
                    return this;

                ArrayBackedSlices newSlices = new ArrayBackedSlices(comparator,
                                                                    Arrays.copyOfRange(starts, i, starts.length),
                                                                    Arrays.copyOfRange(ends, i, ends.length));
                if (cmp > 0)
                    newSlices.starts[0] = newStart;
                return newSlices;
            }
            return Slices.NONE;
        }

        public Slices withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
        {
            for (int i = ends.length - 1; i >= 0; i--)
            {
                if (comparator.compare(newEnd, starts[i]) < 0)
                    continue;

                // newEnd >= start[i].
                int cmp = comparator.compare(newEnd, ends[i]);
                if (i == ends.length - 1 && cmp >= 0)
                    return this;

                ArrayBackedSlices newSlices = new ArrayBackedSlices(comparator,
                                                                    Arrays.copyOfRange(starts, 0, i+1),
                                                                    Arrays.copyOfRange(ends, 0, i+1));
                if (cmp < 0)
                    newSlices.ends[0] = newEnd;
                return newSlices;
            }
            return Slices.NONE;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            outer:
            for (int i = 0; i < starts.length; i++)
            {
                ClusteringPrefix start = starts[i];
                ClusteringPrefix end = ends[i];

                // If this slice start after max or end before min, it can't intersect
                if (compare(start, maxClusteringValues, true) > 0 || compare(end, minClusteringValues, false) < 0)
                    continue;

                // We could safely return true here, but there's a minor optimization: if the first component
                // of the slice is restricted to a single value (typically the slice is [4:5, 4:7]), we can
                // check that the second component falls within the min/max for that component (and repeat for
                // all components).
                for (int j = 0; j < minClusteringValues.size() && j < maxClusteringValues.size(); j++)
                {
                    AbstractType<?> t = comparator.subtype(j);

                    ByteBuffer s = j < start.size() ? start.get(j) : ByteBufferUtil.EMPTY_BYTE_BUFFER;
                    ByteBuffer f = j < end.size() ? end.get(j) : ByteBufferUtil.EMPTY_BYTE_BUFFER;

                    // we already know the first component falls within its min/max range (otherwise we wouldn't get here)
                    if (j > 0 && (j < end.size() && t.compare(f, minClusteringValues.get(j)) < 0 ||
                                  j < start.size() && t.compare(s, maxClusteringValues.get(j)) > 0))
                        continue outer;

                    // if this component isn't equal in the start and finish, we don't need to check any more
                    if (j >= start.size() || j >= end.size() || t.compare(s, f) != 0)
                        break;
                }
                return true;
            }
            return false;
        }

        /** Helper method for intersects() */
        private int compare(ClusteringPrefix sliceBounds, List<ByteBuffer> sstableBounds, boolean isSliceStart)
        {
            for (int i = 0; i < sstableBounds.size(); i++)
            {
                if (i >= sliceBounds.size())
                {
                    // When isSliceStart is true, we're comparing the end of the slice against the min cell name for the sstable,
                    // so the slice is something like [(1, 0), (1, 0)], and the sstable max is something like (1, 0, 1).
                    // We want to return -1 (slice start is smaller than max column name) so that we say the slice intersects.
                    // The opposite is true when dealing with the end slice.  For example, with the same slice and a min
                    // cell name of (1, 0, 1), we want to return 1 (slice end is bigger than min column name).
                    return isSliceStart ? -1 : 1;
                }

                int comparison = comparator.subtype(i).compare(sliceBounds.get(i), sstableBounds.get(i));
                if (comparison != 0)
                    return comparison;
            }

            // the slice bound and sstable bound have been equal in all components so far
            if (sliceBounds.size() > sstableBounds.size())
            {
                // We have the opposite situation from the one described above.  With a slice of [(1, 0), (1, 0)],
                // and a min/max cell name of (1), we want to say the slice start is smaller than the max and the slice
                // end is larger than the min.
                return isSliceStart ? -1 : 1;
            }

            return 0;
        }

        public AtomIterator makeSliceIterator(SeekableAtomIterator iter)
        {
            // TODO: make it work for the reversed order
            return new WrappingAtomIterator(iter)
            {
                private int currentSlice;
                private boolean inSlice;

                private Atom next;

                @Override
                public boolean hasNext()
                {
                    prepareNext();
                    return next != null;
                }

                @Override
                public Atom next()
                {
                    prepareNext();
                    Atom toReturn = next;
                    next = null;
                    return toReturn;
                }

                private void prepareNext()
                {
                    while (true)
                    {
                        if (next != null || !wrapped.hasNext() || currentSlice >= starts.length)
                            return;

                        if (inSlice)
                        {
                            next = wrapped.next();
                            // if the end of the current slice is greater than the next item, that item is to
                            // be returned. Otherwise we're done with this slice.
                            if (comparator.compare(ends[currentSlice], next) >= 0)
                                return;

                            next = null;
                            inSlice = false;
                            ++currentSlice;
                        }
                        else
                        {
                            // Seek to the potential first element for the current slice
                            if (((SeekableAtomIterator)wrapped).seekTo(starts[currentSlice], ends[currentSlice]))
                            {
                                inSlice = true;
                                next = wrapped.next();
                                return;
                            }
                            ++currentSlice;
                        }
                    }
                }
            };
        }

        private class InForwardOrderTester implements InOrderTester
        {
            private int idx;

            public boolean intersects(ClusteringPrefix start, ClusteringPrefix end)
            {
                while (idx < starts.length && comparator.compare(ends[idx], start) < 0)
                    ++idx;

                if (idx == starts.length)
                    return false;

                // We have start <= ends[idx]. We intersects if starts[idx] <= end
                return comparator.compare(starts[idx], end) <= 0;
            }

            public boolean includes(ClusteringPrefix value)
            {
                while (idx < starts.length && comparator.compare(ends[idx], value) < 0)
                    ++idx;

                if (idx == starts.length)
                    return false;

                // We have value <= ends[idx]. The value is included if starts[idx] <= value
                return comparator.compare(starts[idx], value) <= 0;
            }

            public boolean isDone()
            {
                return idx == starts.length;
            }
        }

        private class InReverseOrderTester implements InOrderTester
        {
            public InReverseOrderTester()
            {
            }

            public boolean intersects(ClusteringPrefix start, ClusteringPrefix end)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            public boolean includes(ClusteringPrefix value)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            public boolean isDone()
            {
                return false;
            }
        }
    }

    private static class SelectAllSlices extends Slices
    {
        private static final InOrderTester trivialTester = new InOrderTester()
        {
            public boolean intersects(ClusteringPrefix start, ClusteringPrefix end)
            {
                return true;
            }

            public boolean includes(ClusteringPrefix value)
            {
                return true;
            }

            public boolean isDone()
            {
                return false;
            }
        };

        public int size()
        {
            return 1;
        }

        public boolean hasLowerBound()
        {
            return false;
        }

        public ClusteringPrefix lowerBound()
        {
            return EmptyClusteringPrefix.BOTTOM;
        }

        public boolean hasUpperBound()
        {
            return false;
        }

        public ClusteringPrefix upperBound()
        {
            return EmptyClusteringPrefix.TOP;
        }

        public ClusteringPrefix getStart(int i)
        {
            return EmptyClusteringPrefix.BOTTOM;
        }

        public ClusteringPrefix getEnd(int i)
        {
            return EmptyClusteringPrefix.TOP;
        }

        public Slices withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart)
        {
            return new ArrayBackedSlices(comparator, new ClusteringPrefix[]{ newStart }, new ClusteringPrefix[]{ EmptyClusteringPrefix.TOP });
        }

        public Slices withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
        {
            return new ArrayBackedSlices(comparator, new ClusteringPrefix[]{ EmptyClusteringPrefix.BOTTOM }, new ClusteringPrefix[]{ newEnd });
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return trivialTester;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return true;
        }

        public AtomIterator makeSliceIterator(SeekableAtomIterator iter)
        {
            return iter;
        }
    }

    private static class SelectNoSlices extends Slices
    {
        private static final InOrderTester trivialTester = new InOrderTester()
        {
            public boolean intersects(ClusteringPrefix start, ClusteringPrefix end)
            {
                return false;
            }

            public boolean includes(ClusteringPrefix value)
            {
                return false;
            }

            public boolean isDone()
            {
                return true;
            }
        };

        public int size()
        {
            return 0;
        }

        public boolean hasLowerBound()
        {
            return false;
        }

        public ClusteringPrefix lowerBound()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasUpperBound()
        {
            return false;
        }

        public ClusteringPrefix upperBound()
        {
            throw new UnsupportedOperationException();
        }

        public ClusteringPrefix getStart(int i)
        {
            return null;
        }

        public ClusteringPrefix getEnd(int i)
        {
            return null;
        }

        public Slices withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart)
        {
            return this;
        }

        public Slices withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd)
        {
            return this;
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return trivialTester;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return false;
        }

        public AtomIterator makeSliceIterator(SeekableAtomIterator iter)
        {
            return AtomIterators.emptyIterator(iter.metadata(), iter.partitionKey(), iter.isReverseOrder());
        }
    }
}
