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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.Atom;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.CellPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Represents the legacy layouts: dense/sparse and simple/compound.
 *
 * This is only use to serialize/deserialize the old format.
 */
public class LegacyLayout
{
    /**
     * Flag affecting deserialization behavior.
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public static enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE;
    }

    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    public final static int RANGE_TOMBSTONE_MASK = 0x10;

    private final boolean isDense;
    private final boolean isCompound;
    private final int clusteringSize;

    private final RangeTombstone.Serializer rangeTombstoneSerializer;

    public LegacyLayout(boolean isDense, boolean isCompound, int clusteringSize)
    {
        this.isDense = isDense;
        this.isCompound = isCompound;
        this.clusteringSize = clusteringSize;

        this.rangeTombstoneSerializer = new RangeTombstone.Serializer(this);
    }

    // We call dense a CF for which each component of the comparator is a clustering column, i.e. no
    // component is used to store a regular column names. In other words, non-composite static "thrift"
    // and CQL3 CF are *not* dense.
    public boolean isDense()
    {
        return isDense;
    }

    public boolean isCompound()
    {
        return isCompound;
    }

    public boolean isCQL3Layout()
    {
        return !isDense() && isCompound();
    }

    public static ByteBuffer serializeAsOldComposite(ClusteringPrefix prefix)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static ClusteringComparator clusteringComparatorFromAbstractType(AbstractType<?> type)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public AbstractType<?> abstractTypeFromClusteringComparator(ClusteringComparator comparator)
    {
        // TODO: This actually doesn't work, we'll need the full CFMetaData since we must include
        // the column name and collection stuffs
        throw new UnsupportedOperationException();
    }

    public Deserializer newDeserializer(DataInput in, Descriptor.Version version)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // Note that for the old layout, this will actually discard the cellname parts that are not strictly 
    // part of the clustering prefix. Don't use this if that's not what you want.
    public ISerializer<ClusteringPrefix> discardingClusteringSerializer()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public ISerializer<IndexInfo> indexSerializer()
    {
        return new IndexInfo.Serializer(this);
    }

    public ISSTableSerializer<Atom> oldFormatAtomSerializer()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public Pair<ClusteringPrefix, ColumnDefinition> decodeCellName(ByteBuffer cellname)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public ByteBuffer encodeCellName(ClusteringPrefix clustering, ColumnDefinition column)
    {
        // TODO (handle the case where the column is null
        throw new UnsupportedOperationException();
    }

    public void deserializeCellBody(DataInput in, DeserializedCell cell, ByteBuffer collectionElement, int mask, Flag flag, int expireBefore)
    throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
        {
            // TODO
            throw new UnsupportedOperationException();
            ///long timestampOfLastDelete = in.readLong();
            ///long ts = in.readLong();
            ///ByteBuffer value = ByteBufferUtil.readWithLength(in);
            ///return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
        }
        else if ((mask & COUNTER_UPDATE_MASK) != 0)
        {
            // TODO
            throw new UnsupportedOperationException();
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            assert collectionElement == null;
            cell.isCounter = false;
            cell.ttl = in.readInt();
            cell.localDeletionTime = in.readInt();
            cell.timestamp = in.readLong();
            cell.value = ByteBufferUtil.readWithLength(in);
            cell.path = null;

            // Transform expired cell to tombstone (as it basically saves the value)
            if (cell.localDeletionTime < expireBefore && flag != Flag.PRESERVE_SIZE)
            {
                // The column is now expired, we can safely return a simple tombstone. Note that
                // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
                // we'll fulfil our responsibility to repair.  See discussion at
                // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
                cell.localDeletionTime = cell.localDeletionTime - cell.ttl;
                cell.ttl = 0;
                cell.value = null;
            }

        }
        else
        {
            cell.isCounter = false;
            cell.ttl = 0;
            cell.path = null;

            cell.timestamp = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);

            boolean isDeleted = (mask & DELETION_MASK) != 0;
            cell.value = isDeleted ? null : value;
            cell.localDeletionTime = isDeleted ? ByteBufferUtil.toInt(value) : Integer.MAX_VALUE;
        }
    }

    public void skipCellBody(DataInput in, int mask)
    throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else if ((mask & EXPIRATION_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else
            FileUtils.skipBytesFully(in, 8);

        int length = in.readInt();
        FileUtils.skipBytesFully(in, length);
    }

    //public abstract IVersionedSerializer<ColumnSlice> sliceSerializer();
    //public abstract IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();
    //public abstract DeletionInfo.Serializer deletionInfoSerializer();

    public RangeTombstone.Serializer rangeTombstoneSerializer()
    {
        return rangeTombstoneSerializer;
    }

    public RowIndexEntry.Serializer rowIndexEntrySerializer()
    {
        return new RowIndexEntry.Serializer(this);
    }

    public interface Deserializer
    {
        /**
         * Whether this deserializer is done or not, i.e. whether we're reached the end of row marker.
         */
        public boolean hasNext() throws IOException;
        /**
         * Whether or not some name has been read but not consumed by readNext.
         */
        public boolean hasUnprocessed() throws IOException;
        /**
         * Comparare the next name to read to the provided Composite.
         * This does not consume the next name.
         */
        public int compareNextTo(Clusterable composite) throws IOException;
        public int compareNextPrefixTo(ClusteringPrefix prefix) throws IOException;
        /**
         * Actually consume the next name and return it.
         */
        public ClusteringPrefix readNextClustering() throws IOException;
        public ByteBuffer getNextColumnName();
        public ByteBuffer getNextCollectionElement();

        /**
         * Skip the next name (consuming it). This skip all the name (clustering, column name and collection element).
         */
        public void skipNext() throws IOException;
    }

    public static class DeserializedCell implements Cell
    {
        private boolean isCounter;
        private ByteBuffer value;
        private long timestamp;
        private int localDeletionTime;
        private int ttl;
        private CellPath path;

        public boolean isCounterCell()
        {
            return isCounter;
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
            // TODO
            throw new UnsupportedOperationException();
        }
    }

    // From OnDiskAtom
    //public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serializeForSSTable(OnDiskAtom atom, DataOutputPlus out) throws IOException
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            type.columnSerializer().serialize((Cell)atom, out);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            type.rangeTombstoneSerializer().serializeForSSTable((RangeTombstone)atom, out);
    //        }
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
    //    {
    //        return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
    //    {
    //        Composite name = type.serializer().deserialize(in);
    //        if (name.isEmpty())
    //        {
    //            // SSTableWriter.END_OF_ROW
    //            return null;
    //        }

    //        int b = in.readUnsignedByte();
    //        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
    //            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
    //        else
    //            return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    //    }

    //    public long serializedSizeForSSTable(OnDiskAtom atom)
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            return type.columnSerializer().serializedSize((Cell)atom, TypeSizes.NATIVE);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            return type.rangeTombstoneSerializer().serializedSizeForSSTable((RangeTombstone)atom);
    //        }
    //    }
    //}
}
