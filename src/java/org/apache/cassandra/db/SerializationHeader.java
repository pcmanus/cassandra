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
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.base.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.AtomStats;
import org.apache.cassandra.db.atoms.Cells;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.IMetadataComponentSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SerializationHeader
{
    private static final int DEFAULT_BASE_DELETION = computeDefaultBaseDeletion();

    public static final Serializer serializer = new Serializer();

    private final AbstractType<?> keyType;
    private final List<AbstractType<?>> clusteringTypes;

    private final PartitionColumns columns;
    private final AtomStats stats;

    private final Map<ByteBuffer, AbstractType<?>> typeMap;

    private final long baseTimestamp;
    private final int baseDeletionTime;
    private final int baseTTL;

    private SerializationHeader(AbstractType<?> keyType,
                                List<AbstractType<?>> clusteringTypes,
                                PartitionColumns columns,
                                AtomStats stats,
                                Map<ByteBuffer, AbstractType<?>> typeMap)
    {
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
        this.stats = stats;
        this.typeMap = typeMap;

        // Not that if a given stats is unset, it means that either it's unused (there is
        // no tombstone whatsoever for instance) or that we have no information on it. In
        // that former case, it doesn't matter which base we use but in the former, we use
        // bases that are more likely to provide small encoded values than the default
        // "unset" value.
        this.baseTimestamp = stats.minTimestamp == Cells.NO_TIMESTAMP ? 0 : stats.minTimestamp;
        this.baseDeletionTime = stats.minLocalDeletionTime == Cells.NO_DELETION_TIME ? DEFAULT_BASE_DELETION : stats.minLocalDeletionTime;
        this.baseTTL = stats.minTTL;
    }

    public SerializationHeader(CFMetaData metadata,
                               PartitionColumns columns,
                               AtomStats stats)
    {
        this(metadata.getKeyValidator(),
             typesOf(metadata.clusteringColumns()),
             columns,
             stats,
             null);
    }

    private static List<AbstractType<?>> typesOf(List<ColumnDefinition> columns)
    {
        return ImmutableList.copyOf(Lists.transform(columns, new Function<ColumnDefinition, AbstractType<?>>()
        {
            public AbstractType<?> apply(ColumnDefinition column)
            {
                return column.type;
            }
        }));
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    private static int computeDefaultBaseDeletion()
    {
        // We need a fixed default, but one that is likely to provide small values (close to 0) when
        // substracted to deletion times. Since deletion times are 'the current time in seconds', we
        // use as base Jan 1, 2015 (in seconds).
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
        c.set(Calendar.YEAR, 2015);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return (int)(c.getTimeInMillis() / 1000);
    }

    public AtomStats stats()
    {
        return stats;
    }

    public AbstractType<?> keyType()
    {
        return keyType;
    }

    public List<AbstractType<?>> clusteringTypes()
    {
        return clusteringTypes;
    }

    public Iterator<ColumnDefinition> simpleColumns(boolean isStatic)
    {
        return isStatic
             ? columns.statics.simpleColumns()
             : columns.regulars.simpleColumns();
    }

    public Iterator<ColumnDefinition> complexColumns(boolean isStatic)
    {
        return isStatic
             ? columns.statics.complexColumns()
             : columns.regulars.complexColumns();
    }

    public AbstractType<?> getType(ColumnDefinition column)
    {
        return typeMap == null ? column.type : typeMap.get(column.name.bytes);
    }

    public long encodeTimestamp(long timestamp)
    {
        return timestamp - baseTimestamp;
    }

    public long decodeTimestamp(long timestamp)
    {
        return baseTimestamp + timestamp;
    }

    public int encodeDeletionTime(int deletionTime)
    {
        return deletionTime - baseDeletionTime;
    }

    public int decodeDeletionTime(int deletionTime)
    {
        return baseDeletionTime + deletionTime;
    }

    public int encodeTTL(int ttl)
    {
        return ttl - baseTTL;
    }

    public int decodeTTL(int ttl)
    {
        return baseTTL + ttl;
    }

    public Component toComponent()
    {
        Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
        Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();
        for (ColumnDefinition column : columns.statics)
            staticColumns.put(column.name.bytes, column.type);
        for (ColumnDefinition column : columns.regulars)
            regularColumns.put(column.name.bytes, column.type);
        return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
    }

    /**
     * We need the CFMetadata to properly deserialize a SerializationHeader but it's clunky to pass that to
     * a SSTable component, so we use this temporary object to delay the actual need for the metadata.
     */
    public static class Component extends MetadataComponent
    {
        private final AbstractType<?> keyType;
        private final List<AbstractType<?>> clusteringTypes;
        private final Map<ByteBuffer, AbstractType<?>> staticColumns;
        private final Map<ByteBuffer, AbstractType<?>> regularColumns;
        private final AtomStats stats;

        private Component(AbstractType<?> keyType,
                          List<AbstractType<?>> clusteringTypes,
                          Map<ByteBuffer, AbstractType<?>> staticColumns,
                          Map<ByteBuffer, AbstractType<?>> regularColumns,
                          AtomStats stats)
        {
            this.keyType = keyType;
            this.clusteringTypes = clusteringTypes;
            this.staticColumns = staticColumns;
            this.regularColumns = regularColumns;
            this.stats = stats;
        }

        public MetadataType getType()
        {
            return MetadataType.HEADER;
        }

        public SerializationHeader toHeader(CFMetaData metadata)
        {
            Map<ByteBuffer, AbstractType<?>> typeMap = new HashMap<>(staticColumns.size() + regularColumns.size());
            typeMap.putAll(staticColumns);
            typeMap.putAll(regularColumns);

            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (ByteBuffer name : typeMap.keySet())
            {
                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (column == null)
                {
                    column = metadata.getDroppedColumnDefinition(name);
                    if (column == null)
                        throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
                }
                builder.add(column);
            }
            return new SerializationHeader(keyType, clusteringTypes, builder.build(), stats, typeMap);
        }
    }

    public static class Serializer implements IMetadataComponentSerializer<Component>
    {
        public void serializeForMessaging(SerializationHeader header, DataOutputPlus out, boolean hasStatic) throws IOException
        {
            writeStats(header.stats, out);

            if (hasStatic)
                writeColumns(header.columns.statics, out);
            writeColumns(header.columns.regulars, out);
        }

        public SerializationHeader deserializeForMessaging(DataInput in, CFMetaData metadata, boolean hasStatic) throws IOException
        {
            AtomStats stats = readStats(in);

            AbstractType<?> keyType = metadata.getKeyValidator();
            List<AbstractType<?>> clusteringTypes = typesOf(metadata.clusteringColumns());

            Columns statics = hasStatic ? readColumns(in, metadata) : Columns.NONE;
            Columns regulars = readColumns(in, metadata);

            return new SerializationHeader(keyType, clusteringTypes, new PartitionColumns(statics, regulars), stats, null);
        }

        public long serializedSizeForMessaging(SerializationHeader header, TypeSizes sizes, boolean hasStatic)
        {
            long size = statsSerializedSize(header.stats, sizes);

            if (hasStatic)
                size += sizeofColumns(header.columns.statics, sizes);
            size += sizeofColumns(header.columns.regulars, sizes);
            return size;
        }

        // For SSTables
        public void serialize(Component header, DataOutputPlus out) throws IOException
        {
            writeStats(header.stats, out);

            writeType(header.keyType, out);
            out.writeShort(header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                writeType(type, out);

            writeColumnsWithTypes(header.staticColumns, out);
            writeColumnsWithTypes(header.regularColumns, out);
        }

        // For SSTables
        public Component deserialize(Descriptor.Version version, DataInput in) throws IOException
        {
            AtomStats stats = readStats(in);

            AbstractType<?> keyType = readType(in);
            int size = in.readUnsignedShort();
            List<AbstractType<?>> clusteringTypes = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                clusteringTypes.add(readType(in));

            Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
            Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();

            readColumnsWithType(in, staticColumns);
            readColumnsWithType(in, regularColumns);

            return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        // For SSTables
        public int serializedSize(Component header)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            int size = statsSerializedSize(header.stats, sizes);

            size += sizeofType(header.keyType, sizes);
            size += sizes.sizeof((short)header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                size += sizeofType(type, sizes);

            size += sizeofColumnsWithTypes(header.staticColumns, sizes);
            size += sizeofColumnsWithTypes(header.regularColumns, sizes);
            return size;
        }

        private void writeColumns(Columns columns, DataOutputPlus out) throws IOException
        {
            out.writeShort(columns.columnCount());
            for (ColumnDefinition column : columns)
                ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
        }

        private long sizeofColumns(Columns columns, TypeSizes sizes)
        {
            long size = sizes.sizeof((short)columns.columnCount());
            for (ColumnDefinition column : columns)
                size += sizes.sizeofWithShortLength(column.name.bytes);
            return size;
        }

        private Columns readColumns(DataInput in, CFMetaData metadata) throws IOException
        {
            int length = in.readUnsignedShort();
            ColumnDefinition[] columns = new ColumnDefinition[length];
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (column == null)
                {
                    column = metadata.getDroppedColumnDefinition(name);
                    if (column == null)
                        throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
                }
                columns[i] = column;
            }
            return new Columns(columns);
        }

        private void writeColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, DataOutputPlus out) throws IOException
        {
            out.writeShort(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                ByteBufferUtil.writeWithShortLength(entry.getKey(), out);
                writeType(entry.getValue(), out);
            }
        }

        private long sizeofColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, TypeSizes sizes)
        {
            long size = sizes.sizeof((short)columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                size += sizes.sizeofWithShortLength(entry.getKey());
                size += sizeofType(entry.getValue(), sizes);
            }
            return size;
        }

        private void readColumnsWithType(DataInput in, Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException
        {
            int length = in.readUnsignedShort();
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                typeMap.put(name, readType(in));
            }
        }

        private void writeType(AbstractType<?> type, DataOutputPlus out) throws IOException
        {
            // TODO: we should have a terser serializaion format. Not a big deal though
            ByteBufferUtil.writeWithLength(UTF8Type.instance.decompose(type.toString()), out);
        }

        private AbstractType<?> readType(DataInput in) throws IOException
        {
            ByteBuffer raw = ByteBufferUtil.readWithLength(in);
            return TypeParser.parse(UTF8Type.instance.compose(raw));
        }

        private int sizeofType(AbstractType<?> type, TypeSizes sizes)
        {
            return sizes.sizeofWithLength(UTF8Type.instance.decompose(type.toString()));
        }

        private void writeStats(AtomStats stats, DataOutputPlus out) throws IOException
        {
            out.writeLong(stats.minTimestamp);
            out.writeInt(stats.minLocalDeletionTime);
            out.writeInt(stats.minTTL);
        }

        private int statsSerializedSize(AtomStats stats, TypeSizes sizes)
        {
            return sizes.sizeof(stats.minTimestamp)
                 + sizes.sizeof(stats.minLocalDeletionTime)
                 + sizes.sizeof(stats.minTTL);
        }

        private AtomStats readStats(DataInput in) throws IOException
        {
            long minTimestamp = in.readLong();
            int minLocalDeletionTime = in.readInt();
            int minTTL = in.readInt();
            return new AtomStats(minTimestamp, minLocalDeletionTime, minTTL);
        }
    }
}
