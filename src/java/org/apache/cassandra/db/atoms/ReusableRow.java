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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;

public class ReusableRow extends AbstractReusableRow
{
    private final ReusableClustering clustering;
    private final int nowInSec;

    private final ReusableLivenessInfo liveness = new ReusableLivenessInfo();

    private long maxLiveTimestamp = LivenessInfo.NO_TIMESTAMP;
    private DeletionTime deletion = DeletionTime.LIVE;

    private final RowDataBlock data;
    private final Writer writer;

    public ReusableRow(int clusteringSize, Columns columns, int nowInSec, boolean isCounter)
    {
        this.clustering = new ReusableClustering(clusteringSize);
        this.data = new RowDataBlock(columns, 1, false, isCounter);
        this.nowInSec = nowInSec;
        this.writer = new Writer(data);
    }

    protected RowDataBlock data()
    {
        return data;
    }

    protected int row()
    {
        return 0;
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public LivenessInfo partitionKeyLivenessInfo()
    {
        return liveness;
    }

    public Row markCounterLocalShardsToBeCleared()
    {
        for (Cell cell : this)
        {
            if (!cell.isCounterCell())
                continue;

            ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(cell.value());
            if (marked != cell.value())
                data.setValue(row(), cell.column(), cell.path(), marked);
        }
        return this;
    }

    public long maxLiveTimestamp()
    {
        return maxLiveTimestamp;
    }

    public int nowInSec()
    {
        return nowInSec;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public Row.Writer writer()
    {
        return writer.reset();
    }

    private class Writer extends RowDataBlock.Writer
    {
        public Writer(RowDataBlock data)
        {
            super(data);
        }

        public void writeClusteringValue(ByteBuffer buffer)
        {
            clustering.writer().writeClusteringValue(buffer);
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            ReusableRow.this.liveness.setTo(info);
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            ReusableRow.this.deletion = deletion;
        }

        public void writeMaxLiveTimestamp(long maxLiveTimestamp)
        {
            ReusableRow.this.maxLiveTimestamp = maxLiveTimestamp;
        }

        @Override
        public Writer reset()
        {
            super.reset();
            clustering.reset();
            liveness.reset();
            maxLiveTimestamp = LivenessInfo.NO_TIMESTAMP;
            deletion = DeletionTime.LIVE;
            return this;
        }
    }
}
