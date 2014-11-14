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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Stores updates on a partition.
 */
public class PartitionUpdate implements Iterable<RowUpdate>
{
    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private final CFMetaData metadata;
    private final DecoratedKey key;

    private final DeletionInfo deletionInfo;

    private final List<RowUpdate> rowUpdates;

    private boolean isSorted;
    private boolean hasStatic;

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            DeletionInfo deletionInfo,
                            List<RowUpdate> rowUpdates,
                            boolean isSorted,
                            boolean hasStatic)
    {
        this.metadata = metadata;
        this.key = key;
        this.deletionInfo = deletionInfo;
        this.rowUpdates = rowUpdates;
        this.isSorted = isSorted;
        this.hasStatic = hasStatic;
    }

    public PartitionUpdate(CFMetaData metadata, DecoratedKey key)
    {
        this(metadata, key, DeletionInfo.live(), new ArrayList<RowUpdate>(), false, false);
    }

    public static PartitionUpdate fromBytes(ByteBuffer bytes)
    {
        // This is for paxos and so we need to be able to read the previous ColumnFamily format.
        // The simplest solution is probably to include a version in the paxos table and assume
        // that no version == old serialization format
        // TODO
        throw new UnsupportedOperationException();
    }

    public static ByteBuffer toBytes(PartitionUpdate update)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    public boolean isEmpty()
    {
        return deletionInfo.isLive() && rowUpdates.isEmpty();
    }

    public int operationCount()
    {
        return rowUpdates.size() + deletionInfo().rangeCount() + (deletionInfo().getTopLevelDeletion().isLive() ? 0 : 1);
    }

    public int dataSize()
    {
        // Used to reject batches that are too big (see BatchStatement.verifyBatchSize)
        // TODO
        throw new UnsupportedOperationException();
    }

    public RowUpdate staticRowUpdate()
    {
        if (!isSorted)
            sort();

        return hasStatic ? rowUpdates.get(0) : null;
    }

    public Iterator<RowUpdate> iterator()
    {
        if (!isSorted)
            sort();

        Iterator<RowUpdate> iter = rowUpdates.iterator();
        if (hasStatic)
            iter.next();

        return iter;
    }

    public int rowCount()
    {
        if (!isSorted)
            sort();

        return rowUpdates.size() - (hasStatic ? 1 : 0);
    }

    public PartitionUpdate addAll(PartitionUpdate update)
    {
        assert !isSorted;

        deletionInfo.add(update.deletionInfo);
        rowUpdates.addAll(update.rowUpdates);
        return this;
    }

    public PartitionUpdate add(RowUpdate update)
    {
        assert !isSorted;
        rowUpdates.add(update);
        return this;
    }

    public void validate()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public long maxTimestamp()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    private synchronized void sort()
    {
        if (isSorted || rowUpdates.isEmpty())
            return;

        // Check for duplicate to reconcile. Note that we reconcile in place.
        ClusteringComparator comparator = metadata.comparator;

        // Sort the array - will still potentially contain duplicate (non-reconciled) rows
        Collections.sort(rowUpdates, comparator);

        int previous = 0; // The last element that was set
        for (int current = 1; current < rowUpdates.size(); current++)
        {
            // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
            RowUpdate prev = rowUpdates.get(previous);
            RowUpdate curr = rowUpdates.get(current);
            if (comparator.compare(prev, curr) == 0)
            {
                rowUpdates.set(previous, prev.mergeTo(curr, SecondaryIndexManager.nullUpdater));
            }
            else
            {
                // current != previous, so simply move current just after previous if needs be
                ++previous;
                if (previous != current)
                    rowUpdates.set(previous, curr);
            }
        }

        // previous is on the last value to keep
        for (int i = rowUpdates.size() - 1; i > previous; i++)
            rowUpdates.remove(i);

        isSorted = true;
        hasStatic = rowUpdates.get(0).clustering() == EmptyClusteringPrefix.STATIC_PREFIX;
    }

    public static class PartitionUpdateSerializer implements IVersionedSerializer<PartitionUpdate>
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();

                // if (cf == null)
                // {
                //     out.writeBoolean(false);
                //     return;
                // }

                // out.writeBoolean(true);
                // serializeCfId(cf.id(), out, version);
                // cf.getComparator().deletionInfoSerializer().serialize(cf.deletionInfo(), out, version);
                // ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                // int count = cf.getColumnCount();
                // out.writeInt(count);
                // int written = 0;
                // for (Cell cell : cf)
                // {
                //     columnSerializer.serialize(cell, out);
                //     written++;
                // }
                // assert count == written: "Table had " + count + " columns, but " + written + " written";
            }

            CFMetaData metadata = update.metadata;

            serializeCfId(metadata.cfId, out, version);

            // TODO: we could consider writing the token (provided this is done by the partitioner,
            // LocalPartition and BytesPartitioner wouldn't have to write anything more, and random
            // partition would be a single long)
            ByteBufferUtil.writeWithShortLength(update.partitionKey().getKey(), out);

            metadata.layout().deletionInfoSerializer().serialize(update.deletionInfo(), out, version);
            out.writeInt(update.rowUpdates.size());
            for (RowUpdate row : update.rowUpdates)
                metadata.layout().rowsSerializer().serialize(row, out);
        }

        public PartitionUpdate deserialize(DataInput in, int version, LegacyLayout.Flag flag, DecoratedKey key) throws IOException
        {
            CFMetaData metadata;
            DeletionInfo delInfo;
            List<RowUpdate> updates;
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
                //if (!in.readBoolean())
                //    return null;

                //ColumnFamily cf = factory.create(Schema.instance.getCFMetaData(deserializeCfId(in, version)));

                //if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
                //{
                //    SuperColumns.deserializerSuperColumnFamily(in, cf, flag, version);
                //}
                //else
                //{
                //    cf.delete(cf.getComparator().deletionInfoSerializer().deserialize(in, version));

                //    ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                //    int size = in.readInt();
                //    for (int i = 0; i < size; ++i)
                //        cf.addColumn(columnSerializer.deserialize(in, flag));
                //}
                //return cf;
            }
            else
            {
                metadata = Schema.instance.getCFMetaData(deserializeCfId(in, version));
                key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
                delInfo = metadata.layout().deletionInfoSerializer().deserialize(in, version);
                int size = in.readInt();
                updates = new ArrayList<>(size);
                throw new UnsupportedOperationException();
                //for (int i = 0; i < size; i++)
                //{
                //    metadata.layout().rowsSerializer().deserialize(in, version, flag, writer, metadata);
                //}
            }
        }

        public PartitionUpdate deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            return serializedSize(update, TypeSizes.NATIVE, version);
        }

        public long serializedSize(PartitionUpdate update, TypeSizes sizes, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
                //if (cf == null)
                //{
                //    return typeSizes.sizeof(false);
                //}
                //else
                //{
                //    return typeSizes.sizeof(true)  /* nullness bool */
                //        + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                //        + contentSerializedSize(cf, typeSizes, version);
                //}
            }

            int size = cfIdSerializedSize(update.metadata().cfId,  sizes, version);
            size += ByteBufferUtil.serializedSizeWithShortLength(update.partitionKey().getKey(), sizes);
            size += update.metadata().layout().deletionInfoSerializer().serializedSize(update.deletionInfo(), version);

            size += sizes.sizeof(update.rowUpdates.size());
            for (RowUpdate row : update.rowUpdates)
                size += update.metadata().layout().rowsSerializer().serializedSize(row, sizes);
            return size;
        }

        public void serializeCfId(UUID cfId, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(cfId, out, version);
        }

        public UUID deserializeCfId(DataInput in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            if (Schema.instance.getCF(cfId) == null)
                throw new UnknownColumnFamilyException("Couldn't find cfId=" + cfId, cfId);

            return cfId;
        }

        public int cfIdSerializedSize(UUID cfId, TypeSizes typeSizes, int version)
        {
            return typeSizes.sizeof(cfId);
        }
    }
}
