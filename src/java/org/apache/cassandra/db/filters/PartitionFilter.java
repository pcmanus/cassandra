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
package org.apache.cassandra.db.filters;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.filters.DataLimits;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * A filter over a single partition.
 */
public interface PartitionFilter
{
    public PartitionColumns selectedColumns();

    public PartitionFilter withUpdatedStart(ClusteringComparator comparator, ClusteringPrefix newStart);
    public PartitionFilter withUpdatedEnd(ClusteringComparator comparator, ClusteringPrefix newEnd);

    public boolean isFullyCoveredBy(CachedPartition partition, DataLimits limits, int nowInSec);
    public boolean isHeadFilter();

    public boolean selectsAllPartition();

    // Given another iterator, only return the atom that match this filter
    public AtomIterator filter(AtomIterator iterator);

    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, DecoratedKey key);
    public AtomIterator getSSTableAtomIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry);
    public AtomIterator getAtomIterator(Partition partition);

    public boolean shouldInclude(SSTableReader sstable);

    public String toString(CFMetaData metadata);

    // From old OnDiskAtomFilter
    //    public static class Serializer implements IVersionedSerializer<IDiskAtomFilter>
    //    {
    //        private final CellNameType type;
    //
    //        public Serializer(CellNameType type)
    //        {
    //            this.type = type;
    //        }
    //
    //        public void serialize(IDiskAtomFilter filter, DataOutputPlus out, int version) throws IOException
    //        {
    //            if (filter instanceof SliceQueryFilter)
    //            {
    //                out.writeByte(0);
    //                type.sliceQueryFilterSerializer().serialize((SliceQueryFilter)filter, out, version);
    //            }
    //            else
    //            {
    //                out.writeByte(1);
    //                type.namesQueryFilterSerializer().serialize((NamesQueryFilter)filter, out, version);
    //            }
    //        }
    //
    //        public IDiskAtomFilter deserialize(DataInput in, int version) throws IOException
    //        {
    //            int b = in.readByte();
    //            if (b == 0)
    //            {
    //                return type.sliceQueryFilterSerializer().deserialize(in, version);
    //            }
    //            else
    //            {
    //                assert b == 1;
    //                return type.namesQueryFilterSerializer().deserialize(in, version);
    //            }
    //        }
    //
    //        public long serializedSize(IDiskAtomFilter filter, int version)
    //        {
    //            int size = 1;
    //            if (filter instanceof SliceQueryFilter)
    //                size += type.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)filter, version);
    //            else
    //                size += type.namesQueryFilterSerializer().serializedSize((NamesQueryFilter)filter, version);
    //            return size;
    //        }
    //    }
}
