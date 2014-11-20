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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.SearchIterator;

public abstract class AbstractPartitionData implements Iterable<Row>, Partition
{
    protected final CFMetaData metadata;
    protected final DecoratedKey key;

    protected final DeletionInfo deletionInfo;
    protected final PartitionColumns columns;

    protected Row staticRow;

    protected int rows;

    // row 'i' clustering prefix is composed of the metadata.clusteringColumns.size() elements starting at 'clustering[rows * i]',
    // its timestamp is at 'timestamps[i]' and the row itself is at 'updates[i]'. The index 'i' in timestamps and updates
    // is used for the static row.
    protected ByteBuffer[] clusterings;
    protected long[] timestamps;
    protected final RowDataBlock data;

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    ByteBuffer[] clusterings,
                                    long[] timestamps,
                                    PartitionColumns columns,
                                    RowDataBlock data)
    {
        this.metadata = metadata;
        this.key = key;
        this.deletionInfo = deletionInfo;
        this.clusterings = clusterings;
        this.timestamps = timestamps;
        this.columns = columns;
        this.data = data;
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    PartitionColumns columns,
                                    RowDataBlock data,
                                    int initialRowCapacity)
    {
        this(metadata,
             key,
             deletionInfo,
             new ByteBuffer[initialRowCapacity * metadata.clusteringColumns().size()],
             new long[initialRowCapacity],
             columns,
             data);
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionTime partitionDeletion,
                                    PartitionColumns columns,
                                    int initialRowCapacity)
    {
        this(metadata,
             key,
             new DeletionInfo(partitionDeletion.takeAlias()),
             columns,
             new RowDataBlock(columns.regulars, initialRowCapacity),
             initialRowCapacity);
    }


    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo.getTopLevelDeletion();
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    public Row staticRow()
    {
        return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
    }

    public int rowCount()
    {
        return rows;
    }

    public boolean isEmpty()
    {
        return deletionInfo.isLive() && rows == 0 && staticRow == null;
    }

    public Iterator<Row> iterator()
    {
        return new UnmodifiableIterator<Row>()
        {
            private final ClusteringPrefix clustering = new AbstractClusteringPrefix()
            {
                public int size()
                {
                    return metadata.clusteringColumns().size();
                }

                public ByteBuffer get(int i)
                {
                    int base = row * metadata.clusteringColumns().size();
                    return clusterings[base + i];
                }
            };

            private final AbstractReusableRow reusableRow = new AbstractReusableRow(data)
            {
                protected int row()
                {
                    return row;
                }

                public ClusteringPrefix clustering()
                {
                    return clustering;
                }

                public long timestamp()
                {
                    return timestamps[row];
                }
            };

            private int row = -1;

            public boolean hasNext()
            {
                return row + 1 < rows;
            }

            public Row next()
            {
                ++row;
                return reusableRow;
            }
        };
    }

    public SearchIterator<ClusteringPrefix, Row> searchIterator()
    {
        throw new UnsupportedOperationException();
    }

    public AtomIterator atomIterator(PartitionColumns columns, Slices slices, boolean reversed)
    {
        throw new UnsupportedOperationException();
    }

    protected class Writer extends RowDataBlock.Writer
    {
        public Writer()
        {
            super(data);
        }

        public void setClustering(ClusteringPrefix clustering)
        {
            assert clustering.eoc() == ClusteringPrefix.EOC.NONE;
            int base = row * metadata.clusteringColumns().size();
            for (int i = 0; i < clustering.size(); i++)
                clusterings[base + i] = clustering.get(i);
        }

        public void setTimestamp(long timestamp)
        {
            timestamps[row] = timestamp;
        }
    }

    protected class RangeTombstoneCollector
    {
        private ClusteringPrefix open;
        private DeletionTime data;

        public void addMarker(RangeTombstoneMarker marker)
        {
            ClusteringPrefix clustering = marker.clustering().takeAlias();
            if (marker.isOpenMarker())
            {
                if (open != null)
                    addRangeTombstone(open, clustering, data);
                open = clustering;
                data = marker.delTime().takeAlias();
            }
            else
            {
                assert open != null;
                addRangeTombstone(open, clustering, data);
            }
        }

        private void addRangeTombstone(ClusteringPrefix min, ClusteringPrefix max, DeletionTime dt)
        {
            deletionInfo.add(new RangeTombstone(min, max, dt), metadata.comparator);
        }
    }
}
