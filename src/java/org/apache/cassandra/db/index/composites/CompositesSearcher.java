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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;


public class CompositesSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(CompositesSearcher.class);

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        super(indexManager, columns);
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, CompositesIndex.IndexedEntry entry, ReadCommand command)
    {
        return command.selects(partitionKey, entry.indexedEntryClustering);
    }

    protected UnfilteredPartitionIterator queryDataFromIndex(AbstractSimplePerColumnSecondaryIndex secondaryIdx,
                                                   final DecoratedKey indexKey,
                                                   final RowIterator indexHits,
                                                   final ReadCommand command,
                                                   final OpOrder.Group writeOp)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        assert secondaryIdx instanceof CompositesIndex;
        final CompositesIndex index = (CompositesIndex)secondaryIdx;

        return new UnfilteredPartitionIterator()
        {
            private CompositesIndex.IndexedEntry nextEntry;

            private UnfilteredRowIterator next;

            public boolean isForThrift()
            {
                return command.isForThrift();
            }

            public boolean hasNext()
            {
                return prepareNext();
            }

            public UnfilteredRowIterator next()
            {
                if (next == null)
                    prepareNext();

                UnfilteredRowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                if (next != null)
                    return true;

                if (nextEntry == null)
                {
                    if (!indexHits.hasNext())
                        return false;

                    nextEntry = index.decodeEntry(indexKey, indexHits.next());
                }

                // Gather all index hits belonging to the same partition and query the data for those hits.
                // TODO: it's much more efficient to do 1 read for all hits to the same partition than doing
                // 1 read per index hit. However, this basically mean materializing all hits for a partition
                // in memory so we should consider adding some paging mechanism. However, index hits should
                // be relatively small so it's much better than the previous code that was materializing all
                // *data* for a given partition.
                SortedSet<Clustering> clusterings = new TreeSet<>(baseCfs.getComparator());
                Map<Clustering, CompositesIndex.IndexedEntry> entries = new HashMap<>();
                DecoratedKey partitionKey = baseCfs.partitioner.decorateKey(nextEntry.indexedKey);

                while (nextEntry != null && partitionKey.getKey().equals(nextEntry.indexedKey))
                {
                    // We're queried a slice of the index, but some hits may not match some of the clustering column constraints
                    if (isMatchingEntry(partitionKey, nextEntry, command))
                    {
                        clusterings.add(nextEntry.indexedEntryClustering);
                        entries.put(nextEntry.indexedEntryClustering, nextEntry);
                    }

                    nextEntry = indexHits.hasNext() ? index.decodeEntry(indexKey, indexHits.next()) : null;
                }

                // Because we've eliminated entries that don't match the clustering columns, it's possible we added nothing
                if (clusterings.isEmpty())
                    return prepareNext();

                // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                NamesPartitionFilter filter = new NamesPartitionFilter(command.queriedColumns(), clusterings, false);
                SinglePartitionReadCommand dataCmd = new SinglePartitionNamesCommand(baseCfs.metadata,
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     DataLimits.NONE,
                                                                                     partitionKey,
                                                                                     filter);
                UnfilteredRowIterator dataIter = filterStaleEntries(dataCmd.queryMemtableAndDisk(baseCfs), index, indexKey.getKey(), entries, writeOp);
                if (dataIter.isEmpty())
                {
                    dataIter.close();
                    return prepareNext();
                }

                next = dataIter;
                return true;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                indexHits.close();
                if (next != null)
                    next.close();
            }
        };
    }

    private UnfilteredRowIterator filterStaleEntries(UnfilteredRowIterator dataIter,
                                            final CompositesIndex index,
                                            final ByteBuffer indexValue,
                                            final Map<Clustering, CompositesIndex.IndexedEntry> entries,
                                            final OpOrder.Group writeOp)
    {
        return new WrappingUnfilteredRowIterator(dataIter)
        {
            private Unfiltered next;

            @Override
            public boolean hasNext()
            {
                return prepareNext();
            }

            @Override
            public Unfiltered next()
            {
                if (next == null)
                    prepareNext();

                Unfiltered toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                if (next != null)
                    return true;

                while (next == null && super.hasNext())
                {
                    next = super.next();
                    if (next.kind() != Unfiltered.Kind.ROW)
                        return true;

                    Row row = (Row)next;
                    if (!index.isStale(row, indexValue))
                        return true;

                    // The entry is stale: delete the entry and ignore otherwise
                    index.delete(entries.get(row.clustering()), writeOp, nowInSec());
                    next = null;
                }
                return false;
            }
        };
    }
}
