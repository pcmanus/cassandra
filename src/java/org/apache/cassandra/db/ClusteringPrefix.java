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
import java.util.List;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface ClusteringPrefix extends Clusterable, IMeasurableMemory, Aliasable<ClusteringPrefix>
{
    public static final Serializer serializer = new Serializer();

    public enum EOC
    {
        START(-1), NONE(-1), END(1);

        // If composite p has this EOC and is a strict prefix of composite c, then this
        // the result of the comparison of p and c. Basically, p sorts before c unless
        // it's EOC is END.
        public final int prefixComparisonResult;

        private EOC(int prefixComparisonResult)
        {
            this.prefixComparisonResult = prefixComparisonResult;
        }

        public static EOC from(int eoc)
        {
            return eoc == 0 ? NONE : (eoc < 0 ? START : END);
        }
    }

    public int size();
    public ByteBuffer get(int i);
    public EOC eoc();

    // Used to verify if batches goes over a given size
    public int dataSize();

    public ClusteringPrefix withEOC(EOC eoc);

    public ClusteringPrefix takeAlias();

    public interface Writer
    {
        public void writeEOC(EOC eoc);
        public void writeComponent(int i, ByteBuffer value);
    }

    public static class Serializer
    {
        public void serializeNoEOC(ClusteringPrefix clustering, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            writeHeader(clustering, out);
            for (int i = 0; i < clustering.size(); i++)
            {
                ByteBuffer v = clustering.get(i);
                if (v == null || !v.hasRemaining())
                    continue; // handled in the header

                types.get(i).writeValue(v, out);
            }
        }

        public long serializedSizeNoEOC(ClusteringPrefix clustering, int version, List<AbstractType<?>> types, TypeSizes sizes)
        {
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

        public void deserializeNoEOC(DataInput in, int clusteringSize, EOC eoc, int version, List<AbstractType<?>> types, ClusteringPrefix.Writer writer) throws IOException
        {
            int[] header = readHeader(clusteringSize, in);
            for (int i = 0; i < clusteringSize; i++)
            {
                if (isNull(header, i))
                    writer.writeComponent(i, null);
                else if (isEmpty(header, i))
                    writer.writeComponent(i, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                else
                    writer.writeComponent(i, types.get(i).readValue(in));
            }
            writer.writeEOC(eoc);
        }

        private int headerBytesCount(int size)
        {
            // For each component, we store 2 bit to know if the component is empty or null (or neither).
            // We thus handle 4 component per byte
            return size / 4 + (size % 4 == 0 ? 0 : 1);
        }

        private void writeHeader(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
        {
            int nbBytes = headerBytesCount(clustering.size());
            for (int i = 0; i < nbBytes; i++)
            {
                int b = 0;
                for (int j = 0; j < 4; i++)
                {
                    int c = i * 4 + j;
                    if (c >= clustering.size())
                        return;

                    ByteBuffer v = clustering.get(c);
                    if (v == null)
                        b |= (1 << (j * 2) + 1);
                    else if (!v.hasRemaining())
                        b |= (1 << (j * 2));
                }
                out.writeByte((byte)b);
            }
        }

        private int[] readHeader(int clusteringSize, DataInput in) throws IOException
        {
            int nbBytes = headerBytesCount(clusteringSize);
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
}
