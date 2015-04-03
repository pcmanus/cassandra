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
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A clustering prefix is basically the unit of what a {@link ClusteringComparator} can compare.
 * <p>
 * It holds values for the clustering columns of a table (potentially only a prefix of all of them) and it has
 * a "kind" that allows us to implement slices with inclusive and exclusive bounds.
 * <p>
 * In practice, {@code ClusteringPrefix} is just the common parts to its 2 main subtype: {@link Clustering} and
 * {@link Slice.Bound}, where:
 *   1) {@code Clustering} represents the clustering values for a row, i.e. the values for it's clustering columns.
 *   2) {@code Slice.Bound} represents a bound (start or end) of a slice (of rows).
 * See those classes for more details.
 */
public interface ClusteringPrefix extends Aliasable<ClusteringPrefix>, IMeasurableMemory, Clusterable
{
    public static final Serializer serializer = new Serializer();

    /**
     * The kind of clustering prefix this actually is.
     *
     * The kind {@code STATIC_CLUSTERING} is only implemented by {@link Clustering.STATIC_CLUSTERING}, {@code CLUSTERING} is
     * implemented by the {@link Clustering} class, and the rest is implemented by {@link Slice.Bound}.
     */
    public enum Kind
    {
        // Note that the order of that enum matter. It correspond to the sorting of 2 clusterable whose
        // components are all equals
        EXCL_END_BOUND(-1), INCL_START_BOUND(-1), STATIC_CLUSTERING(-1), CLUSTERING(0), INCL_END_BOUND(1), EXCL_START_BOUND(1);

        // If clusterable c1 has this Kind and is a strict prefix of composite c2, then this
        // is the result of compare(c1, c2). Basically, this is the same as comparing the kind of c1 to
        // CLUSTERING.
        public final int prefixComparisonResult;

        private Kind(int prefixComparisonResult)
        {
            this.prefixComparisonResult = prefixComparisonResult;
        }

        public boolean isStart()
        {
            return this == Kind.EXCL_START_BOUND || this == Kind.INCL_START_BOUND;
        }

        public boolean isInclusive()
        {
            return this != Kind.EXCL_START_BOUND && this != Kind.EXCL_END_BOUND;
        }

        public Kind invert()
        {
            switch (this)
            {
                case EXCL_START_BOUND: return EXCL_END_BOUND;
                case EXCL_END_BOUND: return EXCL_START_BOUND;
                case INCL_END_BOUND: return INCL_START_BOUND;
                case INCL_START_BOUND: return INCL_END_BOUND;
                case CLUSTERING: return CLUSTERING;
                case STATIC_CLUSTERING: return STATIC_CLUSTERING;
                default:
                    throw new IllegalStateException("Missing entry for " + this);
            }
        }
    }



    public Kind kind();

    /**
     * The number of values in this prefix.
     *
     * There can't be more values that the this is a prefix of has of clustering columns.
     *
     * @return the number of values in this prefix.
     */
    public int size();

    /**
     * Retrieves the ith value of this prefix.
     *
     * @param i the index of the value to retrieve. Must be such that {@code 0 <= i < size()}.
     *
     * @return the ith value of this prefix. Note that a value can be {@code null}.
     */
    public ByteBuffer get(int i);

    public void digest(MessageDigest digest);

    // Used to verify if batches goes over a given size
    public int dataSize();

    public String toString(CFMetaData metadata);

    public void writeTo(Writer writer);

    /**
     * Interface for writing a clustering prefix.
     * <p>
     * Each value for the prefix should simply be written in order.
     */
    public interface Writer
    {
        /**
         * Write the next value to the writer.
         *
         * @param value the value to write.
         */
        public void writeClusteringValue(ByteBuffer value);
    }

    public static class Serializer
    {
        public void serialize(ClusteringPrefix clustering, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            // We shouldn't serialize static clusterings
            assert clustering.kind() != Kind.STATIC_CLUSTERING;
            out.writeByte(clustering.kind().ordinal());
            if (clustering.kind() == Kind.CLUSTERING)
                Clustering.serializer.serialize((Clustering)clustering, out, version, types);
            else
                Slice.Bound.serializer.serializeValues((Slice.Bound)clustering, out, version, types);
        }

        public ClusteringPrefix deserialize(DataInput in, int version, List<AbstractType<?>> types) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            // We shouldn't serialize static clusterings
            assert kind != Kind.STATIC_CLUSTERING;
            if (kind == Kind.CLUSTERING)
                return Clustering.serializer.deserialize(in, version, types);
            else
                return Slice.Bound.serializer.deserializeValues(in, kind, version, types);
        }

        public long serializedSize(ClusteringPrefix clustering, int version, List<AbstractType<?>> types, TypeSizes sizes)
        {
            // We shouldn't serialize static clusterings
            assert clustering.kind() != Kind.STATIC_CLUSTERING;
            return 1 + (clustering.kind() == Kind.CLUSTERING
                        ? Clustering.serializer.serializedSize((Clustering)clustering, version, types, sizes)
                        : Slice.Bound.serializer.serializedSizeValues((Slice.Bound)clustering, version, types, sizes));
        }

        void serializeValuesWithoutSize(ClusteringPrefix clustering, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            if (clustering.size() == 0)
                return;

            writeHeader(clustering, out);
            for (int i = 0; i < clustering.size(); i++)
            {
                ByteBuffer v = clustering.get(i);
                if (v == null || !v.hasRemaining())
                    continue; // handled in the header

                types.get(i).writeValue(v, out);
            }
        }

        long valuesWithoutSizeSerializedSize(ClusteringPrefix clustering, int version, List<AbstractType<?>> types, TypeSizes sizes)
        {
            if (clustering.size() == 0)
                return 0;

            long size = headerBytesCount(clustering.size());
            for (int i = 0; i < clustering.size(); i++)
            {
                ByteBuffer v = clustering.get(i);
                if (v == null || !v.hasRemaining())
                    continue; // handled in the header

                size += types.get(i).writtenLength(v, sizes);
            }
            return size;
        }

        void deserializeValuesWithoutSize(DataInput in, int size, int version, List<AbstractType<?>> types, ClusteringPrefix.Writer writer) throws IOException
        {
            if (size == 0)
                return;

            int[] header = readHeader(size, in);
            for (int i = 0; i < size; i++)
            {
                if (isNull(header, i))
                    writer.writeClusteringValue(null);
                else if (isEmpty(header, i))
                    writer.writeClusteringValue(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                else
                    writer.writeClusteringValue(types.get(i).readValue(in));
            }
        }

        private int headerBytesCount(int size)
        {
            // For each component, we store 2 bit to know if the component is empty or null (or neither).
            // We thus handle 4 component per byte
            return size / 4 + (size % 4 == 0 ? 0 : 1);
        }

        /**
         * Whatever the type of a given clustering column is, its value can always be either empty or null. So we at least need to distinguish those
         * 2 values, and because we want to be able to store fixed width values without appending their (fixed) size first, we need a way to encode
         * empty values too. So for that, every clustering prefix includes a "header" that contains 2 bits per element in the prefix. For each element,
         * those 2 bits encode whether the element is null, empty, or none of those.
         */
        private void writeHeader(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
        {
            int nbBytes = headerBytesCount(clustering.size());
            for (int i = 0; i < nbBytes; i++)
            {
                int b = 0;
                for (int j = 0; j < 4; j++)
                {
                    int c = i * 4 + j;
                    if (c >= clustering.size())
                        break;

                    ByteBuffer v = clustering.get(c);
                    if (v == null)
                        b |= (1 << (j * 2) + 1);
                    else if (!v.hasRemaining())
                        b |= (1 << (j * 2));
                }
                out.writeByte((byte)b);
            }
        }

        private int[] readHeader(int size, DataInput in) throws IOException
        {
            int nbBytes = headerBytesCount(size);
            int[] header = new int[nbBytes];
            for (int i = 0; i < nbBytes; i++)
                header[i] = in.readUnsignedByte();
            return header;
        }

        private boolean isNull(int[] header, int i)
        {
            int b = header[i / 4];
            int mask = 1 << ((i % 4) * 2) + 1;
            return (b & mask) != 0;
        }

        private boolean isEmpty(int[] header, int i)
        {
            int b = header[i / 4];
            int mask = 1 << ((i % 4) * 2);
            return (b & mask) != 0;
        }
    }

    /**
     * Helper class that makes the deserialization of clustering prefixes faster.
     * <p>
     * The main reason for this is that when we deserialize rows from sstables, there is many cases where we have
     * a bunch of rows to skip at the beginning of an index block because those rows are before the requested slice.
     * This class make sure we can answer the question "is the next row on disk before the requested slice" with as
     * little work as possible. It does that by providing a comparison method that deserialize only what is needed
     * to decide of the comparison.
     */
    public static class Deserializer
    {
        private final ClusteringComparator comparator;
        private final DataInput in;
        private final SerializationHeader serializationHeader;

        private boolean nextIsRow;
        private int[] nextHeader;

        private int nextSize;
        private ClusteringPrefix.Kind nextKind;
        private int deserializedSize;
        private final ByteBuffer[] nextValues;

        public Deserializer(ClusteringComparator comparator, DataInput in, SerializationHeader header)
        {
            this.comparator = comparator;
            this.in = in;
            this.serializationHeader = header;
            this.nextValues = new ByteBuffer[comparator.size()];
        }

        public void prepare(int flags) throws IOException
        {
            assert !AtomSerializer.isStatic(flags) : "Flags = " + flags;
            this.nextIsRow = AtomSerializer.kind(flags) == Atom.Kind.ROW;
            this.nextSize = nextIsRow ? comparator.size() : in.readUnsignedShort();
            this.nextHeader = serializer.readHeader(nextSize, in);
            this.nextKind = nextIsRow ? Kind.CLUSTERING : AtomSerializer.boundKind(flags);
            this.deserializedSize = 0;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            if (bound == Slice.Bound.TOP)
                return -1;

            if (bound.size() == 0)
                return nextSize == 0 ? 0 : 1;

            if (nextSize == 0)
                return -1;

            for (int i = 0; i < bound.size(); i++)
            {
                if (!hasComponent(i))
                    return nextKind.prefixComparisonResult;

                int cmp = comparator.compareComponent(i, nextValues[i], bound.get(i));
                if (cmp != 0)
                    return cmp;
            }

            if (bound.size() == nextSize)
                return nextKind.compareTo(bound.kind());

            // We know that we'll have exited already if nextSize < bound.size
            return -bound.kind().prefixComparisonResult;
        }

        private boolean hasComponent(int i) throws IOException
        {
            if (i >= nextSize)
                return false;

            while (deserializedSize <= i)
                deserializeOne();

            return true;
        }

        private boolean deserializeOne() throws IOException
        {
            if (deserializedSize == nextSize)
                return false;

            int i = deserializedSize++;
            nextValues[i] = serializer.isNull(nextHeader, i)
                          ? null
                          : (serializer.isEmpty(nextHeader, i) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : serializationHeader.clusteringTypes().get(i).readValue(in));
            return true;
        }

        private void deserializeAll() throws IOException
        {
            while (deserializeOne())
                continue;
        }

        public void deserializeNextBound(Slice.Bound.Writer writer) throws IOException
        {
            assert !nextIsRow;
            deserializeAll();
            for (int i = 0; i < nextSize; i++)
                writer.writeClusteringValue(nextValues[i]);
            writer.writeBoundKind(nextKind);
        }

        public void deserializeNextClustering(Clustering.Writer writer) throws IOException
        {
            assert nextIsRow && nextSize == nextValues.length;
            deserializeAll();
            for (int i = 0; i < nextSize; i++)
                writer.writeClusteringValue(nextValues[i]);
        }

        public void skipNext() throws IOException
        {
            for (int i = deserializedSize; i < nextSize; i++)
                if (!serializer.isNull(nextHeader, i) && !serializer.isEmpty(nextHeader, i))
                    serializationHeader.clusteringTypes().get(i).skipValue(in);
        }
    }
}
