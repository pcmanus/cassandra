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
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Interval;

public class RangeTombstone extends Interval<ClusteringPrefix, DeletionTime>
{
    public RangeTombstone(ClusteringPrefix start, ClusteringPrefix stop, long markedForDeleteAt, int localDeletionTime)
    {
        this(start, stop, new SimpleDeletionTime(markedForDeleteAt, localDeletionTime));
    }

    public RangeTombstone(ClusteringPrefix start, ClusteringPrefix stop, DeletionTime delTime)
    {
        super(start, stop, delTime);
    }

    public int getLocalDeletionTime()
    {
        return data.localDeletionTime();
    }

    public long timestamp()
    {
        return data.markedForDeleteAt();
    }

    // TODO; don't think we need this anymore?
    //public void updateDigest(MessageDigest digest)
    //{
    //    digest.update(min.toByteBuffer().duplicate());
    //    digest.update(max.toByteBuffer().duplicate());

    //    try (DataOutputBuffer buffer = new DataOutputBuffer())
    //    {
    //        buffer.writeLong(data.markedForDeleteAt);
    //        digest.update(buffer.getData(), 0, buffer.getLength());
    //    }
    //    catch (IOException e)
    //    {
    //        throw new RuntimeException(e);
    //    }
    //}

    /**
     * This tombstone supersedes another one if it is more recent and cover a
     * bigger range than rt.
     */
    public boolean supersedes(RangeTombstone rt, Comparator<ClusteringPrefix> comparator)
    {
        if (rt.data.markedForDeleteAt() > data.markedForDeleteAt())
            return false;

        return comparator.compare(min, rt.min) <= 0 && comparator.compare(max, rt.max) >= 0;
    }

    public boolean includes(Comparator<ClusteringPrefix> comparator, ClusteringPrefix name)
    {
        return comparator.compare(name, min) >= 0 && comparator.compare(name, max) <= 0;
    }

    public static class Serializer implements ISSTableSerializer<RangeTombstone>
    {
        private final LegacyLayout layout;

        public Serializer(LegacyLayout layout)
        {
            this.layout = layout;
        }

        public void serializeForSSTable(RangeTombstone t, DataOutputPlus out) throws IOException
        {
            layout.discardingClusteringSerializer().serialize(t.min, out);
            out.writeByte(LegacyLayout.RANGE_TOMBSTONE_MASK);
            layout.discardingClusteringSerializer().serialize(t.max, out);
            DeletionTime.serializer.serialize(t.data, out);
        }

        public RangeTombstone deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
        {
            ClusteringPrefix min = layout.discardingClusteringSerializer().deserialize(in);

            int b = in.readUnsignedByte();
            assert (b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0;
            return deserializeBody(in, min, version);
        }

        public RangeTombstone deserializeBody(DataInput in, ClusteringPrefix min, Descriptor.Version version) throws IOException
        {
            ClusteringPrefix max = layout.discardingClusteringSerializer().deserialize(in);
            DeletionTime dt = DeletionTime.serializer.deserialize(in);
            return new RangeTombstone(min, max, dt);
        }

        public void skipBody(DataInput in, Descriptor.Version version) throws IOException
        {
            layout.discardingClusteringSerializer().deserialize(in);
            DeletionTime.serializer.skip(in);
        }

        public long serializedSizeForSSTable(RangeTombstone t)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            return layout.discardingClusteringSerializer().serializedSize(t.min, typeSizes)
                 + 1 // serialization flag
                 + layout.discardingClusteringSerializer().serializedSize(t.max, typeSizes)
                 + DeletionTime.serializer.serializedSize(t.data, typeSizes);
        }
    }
}
