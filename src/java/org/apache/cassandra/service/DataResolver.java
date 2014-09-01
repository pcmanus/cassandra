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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class DataResolver extends AbstractResolver
{
    private final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<AsyncOneResponse>());

    public DataResolver(Keyspace keyspace, CFMetaData metadata, ConsistencyLevel consistency, int nowInSec, int maxResponseCount)
    {
        super(keyspace, metadata, consistency, nowInSec, maxResponseCount);
    }

    public DataIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return PartitionIterators.asDataIterator(response.makeIterator(), nowInSec);
    }

    public DataIterator resolve()
    {
        List<MessageIn<ReadResponse>> messages = new ArrayList<MessageIn<ReadResponse>>(responses.size());
        List<PartitionIterator> iters = new ArrayList<>(messages.size());
        InetAddress[] sources = new InetAddress[messages.size()];
        for (int i = 0; i < messages.size(); i++)
        {
            MessageIn<ReadResponse> msg = messages.get(i);
            iters.add(msg.payload.makeIterator());
            sources[i] = msg.from;
        }

        return PartitionIterators.mergeAsDataIterator(iters, nowInSec, new RepairMergeListener(sources));
    }

    private class RepairMergeListener implements PartitionIterators.MergeListener
    {
        private final InetAddress[] sources;

        public RepairMergeListener(InetAddress[] sources)
        {
            this.sources = sources;
        }

        public AtomIterators.MergeListener getAtomMergeListener(DecoratedKey partitionKey, AtomIterator[] versions)
        {
            return new MergeListener(partitionKey);
        }

        public void close()
        {
            try
            {
                FBUtilities.waitOnFutures(repairResults, DatabaseDescriptor.getWriteRpcTimeout());
            }
            catch (TimeoutException ex)
            {
                // We got all responses, but timed out while repairing
                int blockFor = consistency.blockFor(keyspace);
                if (Tracing.isTracing())
                    Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                else
                    logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

                throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
            }
        }

        private class MergeListener implements AtomIterators.MergeListener
        {
            private final DecoratedKey partitionKey;
            private final PartitionUpdate[] repairs = new PartitionUpdate[sources.length];

            private final RowUpdate[] currentRows = new RowUpdate[sources.length];
            private ClusteringPrefix currentClustering;
            private ColumnDefinition currentColumn;

            private final ClusteringPrefix[] markerOpen = new ClusteringPrefix[sources.length];
            private final DeletionTime[] markerTime = new DeletionTime[sources.length];

            public MergeListener(DecoratedKey partitionKey)
            {
                this.partitionKey = partitionKey;
            }

            private PartitionUpdate update(int i)
            {
                PartitionUpdate upd = repairs[i];
                if (upd == null)
                {
                    upd = new PartitionUpdate(metadata, partitionKey);
                    repairs[i] = upd;
                }
                return upd;
            }

            private RowUpdate currentRow(int i)
            {
                RowUpdate upd = currentRows[i];
                if (upd == null)
                {
                    upd = RowUpdates.create(metadata, currentClustering);
                    currentRows[i] = upd;
                }
                return upd;
            }

            public void onMergingRows(ClusteringPrefix clustering, long mergedTimestamp, Row[] versions)
            {
                currentClustering = clustering;
                for (int i = 0; i < versions.length; i++)
                {
                    long timestamp = versions[i].timestamp();
                    if (mergedTimestamp > timestamp)
                        currentRow(i).updateRowTimestamp(mergedTimestamp);
                }
            }

            public void onMergedColumns(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTimeArray versions)
            {
                currentColumn = c;
                for (int i = 0; i < versions.size(); i++)
                {
                    if (versions.supersedes(i, mergedCompositeDeletion))
                        currentRow(i).updateComplexDeletion(c, mergedCompositeDeletion);
                }
            }

            public void onMergedCells(Cell mergedCell, Cell[] versions)
            {
                for (int i = 0; i < versions.length; i++)
                {
                    Cell version = versions[i];
                    Cell toAdd = version == null ? mergedCell : Cells.diff(mergedCell, version);
                    if (toAdd != null)
                        currentRow(i).addCell(currentColumn, toAdd);
                }
            }

            public void onRowDone()
            {
                for (int i = 0; i < currentRows.length; i++)
                {
                    if (currentRows[i] != null)
                        update(i).add(currentRows[i]);
                }
                Arrays.fill(currentRows, null);
            }

            public void onMergedRangeTombstoneMarkers(ClusteringPrefix clustering, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions)
            {
                for (int i = 0; i < versions.length; i++)
                {
                    RangeTombstoneMarker marker = versions[i];
                    if (isOpenMarker)
                    {
                        if (marker == null || mergedDelTime.supersedes(marker.delTime()))
                        {
                            markerOpen[i] = clustering.takeAlias();
                            markerTime[i] = mergedDelTime.takeAlias();
                        }
                    }
                    else if (markerOpen[i] != null)
                    {
                        update(i).deletionInfo().add(new RangeTombstone(markerOpen[i], clustering.takeAlias(), markerTime[i]), metadata.comparator);
                        markerOpen[i] = null;
                    }
                }
            }

            public void close()
            {
                for (int i = 0; i < repairs.length; i++)
                {
                    if (repairs[i] == null)
                        continue;

                    MessageOut<Mutation> msg = new Mutation(repairs[i]).createMessage(MessagingService.Verb.READ_REPAIR);
                    repairResults.add(MessagingService.instance().sendRR(msg, sources[i]));
                }
            }
        }
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }
}
