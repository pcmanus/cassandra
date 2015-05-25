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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        super(indexManager, columns);
    }

    protected UnfilteredPartitionIterator queryDataFromIndex(final AbstractSimplePerColumnSecondaryIndex index,
                                                   final DecoratedKey indexKey,
                                                   final RowIterator indexHits,
                                                   final ReadCommand command,
                                                   final OpOrder.Group writeOp)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        return new UnfilteredPartitionIterator()
        {
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
                while (next == null && indexHits.hasNext())
                {
                    Row hit = indexHits.next();
                    DecoratedKey key = baseCfs.partitioner.decorateKey(hit.clustering().get(0));

                    SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(true,
                                                                                           baseCfs.metadata,
                                                                                           command.nowInSec(),
                                                                                           command.columnFilter(),
                                                                                           DataLimits.NONE,
                                                                                           key,
                                                                                           command.partitionFilter(key));
                    UnfilteredRowIterator dataIter = filterIfStale(dataCmd.queryMemtableAndDisk(baseCfs),
                                                          index,
                                                          hit,
                                                          indexKey.getKey(),
                                                          writeOp);
                    if (dataIter != null && dataIter.isEmpty())
                    {
                        dataIter.close();
                    }
                    else
                    {
                        next = dataIter;
                    }
                }

                return next != null;
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

    private UnfilteredRowIterator filterIfStale(UnfilteredRowIterator iterator,
                                       AbstractSimplePerColumnSecondaryIndex index,
                                       Row indexHit,
                                       ByteBuffer indexedValue,
                                       OpOrder.Group writeOp)
    {
        // We need to materialize the partition to check if the index columns value
        // is stale or not
        ArrayBackedPartition result = ArrayBackedPartition.create(iterator);
        Clustering clustering = new SimpleClustering(index.indexedColumn().name.bytes);
        Row data = result.getRow(clustering);
        Cell c = data == null ? null : data.getCell(baseCfs.metadata.compactValueColumn());
        if (c == null || !c.isLive(iterator.nowInSec()) || index.indexedColumn().type.compare(indexedValue, c.value()) != 0)
        {
            // Index is stale, remove the index entry and ignore
            index.delete(iterator.partitionKey().getKey(),
                         clustering,
                         indexedValue,
                         null,
                         new SimpleDeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), iterator.nowInSec()),
                         writeOp,
                         iterator.nowInSec());
            return null;
        }
        return result.unfilteredIterator();
    }
}
