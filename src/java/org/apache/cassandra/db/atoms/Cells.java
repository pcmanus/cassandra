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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.FBUtilities;

public abstract class Cells
{
    public static final int MAX_TTL_SEC = 20 * 365 * 24 * 60 * 60; // 20 years in seconds

    public static final int NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int NO_TTL = 0;
    public static final int NO_DELETION_TIME = Integer.MAX_VALUE;

    private Cells() {}

    public static boolean isLive(Cell cell, int nowInSec)
    {
        return nowInSec < cell.localDeletionTime();
    }

    public static Cell create(ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        return create(value, timestamp, ttl, ttl > 0 ? FBUtilities.nowInSeconds() : NO_DELETION_TIME, metadata);
    }

    public static Cell create(ByteBuffer value, long timestamp, int ttl, int localDelTime, CFMetaData metadata)
    {
        return create(null, value, timestamp, ttl, localDelTime, metadata);
    }

    public static Cell create(CellPath path, ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        return create(path, value, timestamp, ttl, ttl > 0 ? (int)(System.currentTimeMillis() / 1000) : Integer.MAX_VALUE, metadata);
    }

    public static Cell create(CellPath path, ByteBuffer value, long timestamp, int ttl, int localDelTime, CFMetaData metadata)
    {
        assert ttl <= 0 || localDelTime == Integer.MAX_VALUE;
        if (ttl <= 0)
            ttl = metadata.getDefaultTimeToLive();

        return new SimpleCell(value, timestamp, localDelTime, ttl, path);
    }

    public static Cell createCounterUpdate(long delta, long timestamp)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Cell createCounter(ByteBuffer value, long timestamp)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Cell createTombsone(long timestamp)
    {
        return createTombsone(FBUtilities.nowInSeconds(), timestamp);
    }

    public static Cell createTombsone(int localDeletionTime, long timestamp)
    {
        return createTombsone(null, localDeletionTime, timestamp);
    }

    public static Cell createTombsone(CellPath path, int localDeletionTime, long timestamp)
    {
        return new SimpleCell(null, timestamp, localDeletionTime, NO_TTL, path);
    }

    public static Cell diff(Cell merged, Cell cell)
    {
        // TODO
        throw new UnsupportedOperationException();

        // For counters
        //assert this instanceof CounterCell : "Wrong class type: " + getClass();

        //if (timestamp() < cell.timestamp())
        //    return cell;

        //// Note that if at that point, cell can't be a tombstone. Indeed,
        //// cell is the result of merging us with other nodes results, and
        //// merging a CounterCell with a tombstone never return a tombstone
        //// unless that tombstone timestamp is greater that the CounterCell
        //// one.
        //assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

        //if (((CounterCell) this).timestampOfLastDelete() < ((CounterCell) cell).timestampOfLastDelete())
        //    return cell;

        //CounterContext.Relationship rel = CounterCell.contextManager.diff(cell.value(), value());
        //return (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT) ? cell : null;
    }

    public static Cell reconcile(Cell c1, Cell c2, int nowInSec)
    {
        if (c1.isCounterCell())
        {
            assert c2.isCounterCell();
            throw new UnsupportedOperationException();
        }

        long ts1 = c1.timestamp(), ts2 = c2.timestamp();
        if (ts1 != ts2)
            return ts1 < ts2 ? c2 : c1;
        boolean c1Live = isLive(c1, nowInSec);
        if (c1Live != isLive(c2, nowInSec))
            return c1Live ? c1 : c2;
        return c1.value().compareTo(c2.value()) < 0 ? c1 : c2;


        // TODO
        // For Counters
        //assert this instanceof CounterCell : "Wrong class type: " + getClass();

        //// No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        //if (cell instanceof DeletedCell)
        //    return cell;

        //assert (cell instanceof CounterCell) : "Wrong class type: " + cell.getClass();

        //// live < live last delete
        //if (timestamp() < ((CounterCell) cell).timestampOfLastDelete())
        //    return cell;

        //long timestampOfLastDelete = ((CounterCell) this).timestampOfLastDelete();

        //// live last delete > live
        //if (timestampOfLastDelete > cell.timestamp())
        //    return this;

        //// live + live. return one of the cells if its context is a superset of the other's, or merge them otherwise
        //ByteBuffer context = CounterCell.contextManager.merge(value(), cell.value());
        //if (context == value() && timestamp() >= cell.timestamp() && timestampOfLastDelete >= ((CounterCell) cell).timestampOfLastDelete())
        //    return this;
        //else if (context == cell.value() && cell.timestamp() >= timestamp() && ((CounterCell) cell).timestampOfLastDelete() >= timestampOfLastDelete)
        //    return cell;
        //else // merge clocks and timestamps.
        //    return new BufferCounterCell(name(),
        //                                 context,
        //                                 Math.max(timestamp(), cell.timestamp()),
        //                                 Math.max(timestampOfLastDelete, ((CounterCell) cell).timestampOfLastDelete()));
    }

    // TODO: we could have more specialized version of cells...
    private static class SimpleCell implements Cell
    {
        private final ByteBuffer value;
        private final long timestamp;
        private final int localDeletionTime;
        private final int ttl;
        private final CellPath path;

        private SimpleCell(ByteBuffer value, long timestamp, int localDeletionTime, int ttl, CellPath path)
        {
            this.value = value;
            this.timestamp = timestamp;
            this.localDeletionTime = localDeletionTime;
            this.ttl = ttl;
            this.path = path;
        }

        public boolean isCounterCell()
        {
            return false;
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
            return localDeletionTime;
        }

        public int ttl()
        {
            return ttl;
        }

        public CellPath path()
        {
            return path;
        }

        public Cell takeAlias()
        {
            return this;
        }
    }
}
