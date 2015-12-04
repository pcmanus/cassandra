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
package org.apache.cassandra.db.aggregation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A <code>GroupMaker</code> can be used to determine if some sorted rows belongs to the same group or not.
 */
public abstract class GroupMaker
{
    /**
     * <code>GroupMaker</code> state.
     *
     * <p>The state contains the primary key of the last row that has been processed by the previous
     * <code>GroupMaker</code> used. It can be passed to a <code>GroupMaker</code> to allow to resuming the grouping.
     * </p>
     */
    public static final class State
    {
        public static final Serializer serializer = new Serializer();

        public static final State EMPTY_STATE = new State(null, null);

        /**
         * The last row partition key.
         */
        private final ByteBuffer partitionKey;

        /**
         * The last row clustering
         */
        private final Clustering clustering;

        public State(ByteBuffer partitionKey, Clustering clustering)
        {
            this.partitionKey = partitionKey;
            this.clustering = clustering;
        }

        /**
         * Returns the last row partition key or <code>null</code> if no rows has been processed yet.
         * @return the last row partition key or <code>null</code> if no rows has been processed yet
         */
        public ByteBuffer partitionKey()
        {
            return partitionKey;
        }

        /**
         * Returns the last row partition key or <code>null</code> if either no rows has been processed yet or the last
         * row was a static row.
         * @return he last row partition key or <code>null</code> if either no rows has been processed yet or the last
         * row was a static row
         */
        public Clustering clustering()
        {
            return clustering;
        }

        /**
         * Checks if the state contains a Clustering for the last row that has been processed.
         * @return <code>true</code> if the state contains a Clustering for the last row that has been processed,
         * <code>false</code> otherwise.
         */
        public boolean hasClustering()
        {
            return clustering != null;
        }

        public static class Serializer
        {
            public void serialize(State state, DataOutputPlus out, int version) throws IOException
            {
                boolean hasPartitionKey = state.partitionKey != null;
                out.writeBoolean(hasPartitionKey);
                if (hasPartitionKey)
                {
                    ByteBufferUtil.writeWithVIntLength(state.partitionKey, out);
                    boolean hasClustering = state.hasClustering();
                    out.writeBoolean(hasClustering);
                    if (hasClustering)
                    {
                        ByteBuffer[] values = state.clustering.getRawValues();
                        int size = values.length;
                        out.writeUnsignedVInt(size);
                        for (ByteBuffer value : values)
                            ByteBufferUtil.writeWithVIntLength(value, out);
                    }
                }
            }

            public State deserialize(DataInputPlus in, int version) throws IOException
            {
                if (!in.readBoolean())
                    return State.EMPTY_STATE;

                ByteBuffer partitionKey = ByteBufferUtil.readWithVIntLength(in);
                Clustering clustering = null;
                if (in.readBoolean())
                {
                    int size = (int) in.readUnsignedVInt();
                    ByteBuffer[] values = new ByteBuffer[size];
                    for (int i = 0; i < size; i++)
                        values[i] = ByteBufferUtil.readWithVIntLength(in);
                    clustering = Clustering.make(values);
                }
                return new State(partitionKey, clustering);
            }

            public long serializedSize(State state, int version)
            {
                boolean hasPartitionKey = state.partitionKey != null;
                long size = TypeSizes.sizeof(hasPartitionKey);
                if (hasPartitionKey)
                {
                    size += ByteBufferUtil.serializedSizeWithVIntLength(state.partitionKey);
                    boolean hasClustering = state.hasClustering();
                    size += TypeSizes.sizeof(hasClustering);
                    if (hasClustering)
                    {
                        ByteBuffer[] values = state.clustering.getRawValues();
                        size += TypeSizes.sizeofUnsignedVInt(values.length);
                        for (ByteBuffer value : values)
                            size += ByteBufferUtil.serializedSizeWithVIntLength(value);
                    }
                }
                return size;
            }
        }
    }

    /**
     * <code>GroupMaker</code> that groups all the rows together.
     */
    public static final GroupMaker GROUP_EVERYTHING = new GroupMaker()
    {
        public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering)
        {
            return false;
        }

        public boolean returnAtLeastOneRow()
        {
            return true;
        }
    };

    public static GroupMaker newInstance(int clusteringPrefixSize, State state)
    {
        return new PkPrefixGroupMaker(clusteringPrefixSize, state);
    }

    public static GroupMaker newInstance(int clusteringPrefixSize)
    {
        return new PkPrefixGroupMaker(clusteringPrefixSize);
    }

    /**
     * Checks if a given row belongs to the same group that the previous row or not.
     *
     * @param partitionKey the partition key.
     * @param clustering the row clustering key
     * @return <code>true</code> if the row belongs to the same group that the previous one, <code>false</code>
     * otherwise.
     */
    public abstract boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering);

    /**
     * Specify if at least one row must be returned. If the selection is performing some aggregations on all the rows,
     * one row should be returned even if no records were processed.
     *
     * @return <code>true</code> if at least one row must be returned, <code>false</code> otherwise.
     */
    public boolean returnAtLeastOneRow()
    {
        return false;
    }

    private static final class PkPrefixGroupMaker extends GroupMaker
    {
        /**
         * The last partition key seen
         */
        private ByteBuffer lastPartitionKey;

        /**
         * The last clustering prefix seen
         */
        private final ByteBuffer[] lastClusteringPrefix;

        public PkPrefixGroupMaker(int clusteringPrefixSize, State state)
        {
            this(clusteringPrefixSize);
            this.lastPartitionKey = state.partitionKey();
            if (state.hasClustering())
            {
                ByteBuffer[] clustering = state.clustering.getRawValues();
                for (int i = 0; i < clusteringPrefixSize; i++)
                {
                    lastClusteringPrefix[i] = clustering[i];
                }
            }
        }

        public PkPrefixGroupMaker(int clusteringPrefixSize)
        {
            this.lastClusteringPrefix = new ByteBuffer[clusteringPrefixSize];
        }

        @Override
        public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering)
        {
            boolean isNew = false;

            if (!partitionKey.getKey().equals(lastPartitionKey))
            {
                isNew = true;
                lastPartitionKey = partitionKey.getKey();
                if (Clustering.STATIC_CLUSTERING == clustering)
                {
                    Arrays.fill(lastClusteringPrefix, null);
                    return true;
                }
            }

            for (int i = 0; i < lastClusteringPrefix.length; i++)
            {
                ByteBuffer byteBuffer = clustering.get(i);

                if (isNew)
                {
                    lastClusteringPrefix[i] = byteBuffer;
                }
                else if (!byteBuffer.equals(lastClusteringPrefix[i]))
                {
                    isNew = true;
                    lastClusteringPrefix[i] = byteBuffer;
                }
            }
            return isNew;
        }
    }
}