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
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SuperColumns
{
    public static Iterator<OnDiskAtom> onDiskIterator(DataInput dis, int superColumnCount, IColumnSerializer.Flag flag, int expireBefore)
    {
        return new SCIterator(dis, superColumnCount, flag, expireBefore);
    }

    public static void serializeSuperColumnFamily(ColumnFamily scf, DataOutput dos, int version) throws IOException
    {
        /*
         * There is 2 complications:
         *   1) We need to know the number of super columns in the column
         *   family to write in the header (so we do a first pass to group
         *   columns before serializing).
         *   2) For deletion infos, we need to figure out which are top-level
         *   deletions and which are super columns deletions (i.e. the
         *   subcolumns range deletions).
         */

        Map<ByteBuffer, List<IColumn>> scMap = groupSuperColumns(scf);
        DeletionInfo delInfo = scf.deletionInfo();

        // Actually Serialize
        DeletionInfo.serializer().serialize(new DeletionInfo(delInfo.getTopLevelDeletion()), dos, version);
        for (Map.Entry<ByteBuffer, List<IColumn>> entry : scMap.entrySet())
        {
            ByteBufferUtil.writeWithShortLength(entry.getKey(), dos);

            List<DeletionTime> delTimes = delInfo.rangeCovering(entry.getKey());
            assert delTimes.size() <= 1; // We're supposed to have either no deletion, or a full SC deletion.
            DeletionInfo scDelInfo = delTimes.isEmpty() ? DeletionInfo.LIVE : new DeletionInfo(delTimes.get(0));
            DeletionInfo.serializer().serialize(scDelInfo, dos, MessagingService.VERSION_10);

            dos.writeInt(entry.getValue().size());
            for (IColumn subColumn : entry.getValue())
                Column.serializer().serialize(subColumn, dos);
        }
    }

    private static Map<ByteBuffer, List<IColumn>> groupSuperColumns(ColumnFamily scf)
    {
        CompositeType type = (CompositeType)scf.getComparator();
        Map<ByteBuffer, List<IColumn>> scMap = new HashMap<ByteBuffer, List<IColumn>>();

        ByteBuffer scName = null;
        List<IColumn> subColumns = null;
        for (IColumn column : scf)
        {
            ByteBuffer components[] = type.split(column.name());
            assert components.length == 2;

            if (scName == null || type.types.get(0).compare(scName, components[0]) != 0)
            {
                // new super column
                scName = components[0];
                subColumns = new ArrayList<IColumn>();
                scMap.put(scName, subColumns);
            }

            subColumns.add(((Column)column).withUpdatedName(components[1]));
        }
        return scMap;
    }

    public static void deserializerSuperColumnFamily(DataInput dis, ColumnFamily cf, IColumnSerializer.Flag flag, int expireBefore, int version) throws IOException
    {
        // Note that there was no way to insert a range tombstone in a SCF in 1.2
        cf.delete(DeletionInfo.serializer().deserialize(dis, version, cf.getComparator()));
        assert !cf.deletionInfo().rangeIterator().hasNext();

        Iterator<OnDiskAtom> iter = onDiskIterator(dis, dis.readInt(), flag, expireBefore);
        while (iter.hasNext())
            cf.addAtom(iter.next());
    }

    public static long serializedSize(ColumnFamily scf, TypeSizes typeSizes, int version)
    {
        Map<ByteBuffer, List<IColumn>> scMap = groupSuperColumns(scf);
        DeletionInfo delInfo = scf.deletionInfo();

        // Actually Serialize
        long size = DeletionInfo.serializer().serializedSize(new DeletionInfo(delInfo.getTopLevelDeletion()), version);
        for (Map.Entry<ByteBuffer, List<IColumn>> entry : scMap.entrySet())
        {
            int nameSize = entry.getKey().remaining();
            size += typeSizes.sizeof((short) nameSize) + nameSize;

            List<DeletionTime> delTimes = delInfo.rangeCovering(entry.getKey());
            assert delTimes.size() <= 1; // We're supposed to have either no deletion, or a full SC deletion.
            DeletionInfo scDelInfo = delTimes.isEmpty() ? DeletionInfo.LIVE : new DeletionInfo(delTimes.get(0));
            size += DeletionInfo.serializer().serializedSize(scDelInfo, MessagingService.VERSION_10);

            size += typeSizes.sizeof(entry.getValue().size());
            for (IColumn subColumn : entry.getValue())
                size += Column.serializer().serializedSize(subColumn, typeSizes);
        }
        return size;
    }

    private static class SCIterator implements Iterator<OnDiskAtom>
    {
        private final DataInput dis;
        private final int scCount;

        private final IColumnSerializer.Flag flag;
        private final int expireBefore;

        private int read;
        private Iterator<Column> subColumnsIterator;

        private SCIterator(DataInput dis, int superColumnCount, IColumnSerializer.Flag flag, int expireBefore)
        {
            this.dis = dis;
            this.scCount = superColumnCount;
            this.flag = flag;
            this.expireBefore = expireBefore;
        }

        public boolean hasNext()
        {
            return (subColumnsIterator != null && subColumnsIterator.hasNext()) || read < scCount;
        }

        public OnDiskAtom next()
        {
            try
            {
                if (subColumnsIterator.hasNext())
                    return subColumnsIterator.next();

                // Read one more super column
                ++read;

                ByteBuffer scName = ByteBufferUtil.readWithShortLength(dis);
                DeletionInfo delInfo = DeletionInfo.serializer().deserialize(dis, MessagingService.VERSION_10, null);
                assert !delInfo.rangeIterator().hasNext(); // We assume no range tombstone (there was no way to insert some in a SCF in 1.2)

                /* read the number of columns */
                int size = dis.readInt();
                List<Column> subColumns = new ArrayList<Column>(size);

                for (int i = 0; i < size; ++i)
                    subColumns.add(Column.serializer().deserialize(dis, flag, expireBefore));

                subColumnsIterator = subColumns.iterator();

                // If the SC was deleted, return that first, otherwise return the first subcolumn
                DeletionTime dtime = delInfo.getTopLevelDeletion();
                if (dtime.equals(DeletionTime.LIVE))
                {
                    return subColumnsIterator.next();
                }
                else
                {
                    return new RangeTombstone(startOf(scName), endOf(scName), dtime);
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    // We don't use CompositeType.Builder mostly because we want to avoid having to provide the comparator.
    private static ByteBuffer startOf(ByteBuffer scName)
    {
        int length = scName.remaining();
        ByteBuffer bb = ByteBuffer.allocate(2 + length + 1);

        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
        bb.put(scName.duplicate());
        bb.put((byte) 0);
        bb.flip();
        return bb;
    }

    private static ByteBuffer endOf(ByteBuffer scName)
    {
        ByteBuffer bb = startOf(scName);
        bb.put(bb.remaining() - 1, (byte)1);
        return bb;
    }
}
