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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.utils.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Functions to deal with the old format.
 *
 * Note: we mostly don't support the old format, but we still use the old cell
 * name encoding in 2 places:
 *   1) in PagingState: we still support native protocol version 3 which use
 *      a cellname-encoded value.
 *   2) in the counter cache saved files: we use the old cell name format as a
 *      way to shove both the clustering, column name and optional cell path
 *      into a ByteBuffer. We could completely replace this by writing each
 *      element separatly, which would actually be faster, but we'd have to deal
 *      with versioning of the saved file so we keep it that way for now.
 */
public abstract class LegacyLayout
{
    private LegacyLayout() {}

    private static LegacyCellName decodeForSuperColumn(CFMetaData metadata, Clustering clustering, ByteBuffer subcol)
    {
        ColumnDefinition def = metadata.getColumnDefinition(subcol);
        if (def != null)
        {
            // it's a statically defined subcolumn
            return new LegacyCellName(clustering, def, null);
        }

        def = metadata.compactValueColumn();
        assert def != null && def.type instanceof MapType;
        return new LegacyCellName(clustering, def, subcol);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname) throws UnknownColumnException
    {
        Clustering clustering = decodeClustering(metadata, cellname);

        if (metadata.isSuper())
            return decodeForSuperColumn(metadata, clustering, CompositeType.extractComponent(cellname, 1));

        if (metadata.isDense() || (metadata.isCompactTable()))
            return new LegacyCellName(clustering, metadata.compactValueColumn(), null);

        ByteBuffer column = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size()) : cellname;
        if (column == null)
        {
            // Tables for composite 2ndary indexes used to be compound but dense, but we've transformed them into regular tables
            // (non compact ones) but with no regular column (i.e. we only care about the clustering). So we'll get here
            // in that case, and what we want to return is basically a row marker.
            if (metadata.partitionColumns().isEmpty())
                return new LegacyCellName(clustering, null, null);

            // Otherwise, we shouldn't get there
            throw new IllegalArgumentException("No column name component found in cell name");
        }

        // Row marker, this is ok
        if (!column.hasRemaining())
            return new LegacyCellName(clustering, null, null);

        ColumnDefinition def = metadata.getColumnDefinition(column);
        if ((def == null) || def.isPrimaryKeyColumn())
        {
            // If it's a compact table, it means the column is in fact a "dynamic" one
            if (metadata.isCompactTable())
                return new LegacyCellName(Clustering.make(column), metadata.compactValueColumn(), null);

            if (def == null)
                throw new UnknownColumnException(metadata, column);
            else
                throw new IllegalArgumentException("Cannot add primary key column to partition update");
        }

        ByteBuffer collectionElement = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size() + 1) : null;

        // Note that because static compact columns are translated to static defs in the new world order, we need to force a static
        // clustering if the definition is static (as it might not be in this case).
        return new LegacyCellName(def.isStatic() ? Clustering.STATIC_CLUSTERING : clustering, def, collectionElement);
    }

    public static ByteBuffer encodeCellName(CFMetaData metadata, ClusteringPrefix clustering, ByteBuffer columnName, ByteBuffer collectionElement)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!metadata.isCompound())
        {
            if (isStatic)
                return columnName;

            assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();
            return clustering.get(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (metadata.isDense() ? 0 : 1) + (collectionElement == null ? 0 : 1);
        if (metadata.isSuper())
            size = clusteringSize + 1;
        ByteBuffer[] values = new ByteBuffer[size];
        for (int i = 0; i < clusteringSize; i++)
        {
            if (isStatic)
            {
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            ByteBuffer v = clustering.get(i);
            // we can have null (only for dense compound tables for backward compatibility reasons) but that
            // means we're done and should stop there as far as building the composite is concerned.
            if (v == null)
                return CompositeType.build(Arrays.copyOfRange(values, 0, i));

            values[i] = v;
        }

        if (metadata.isSuper())
        {
            // We need to set the "column" (in thrift terms) name, i.e. the value corresponding to the subcomparator.
            // What it is depends if this a cell for a declared "static" column or a "dynamic" column part of the
            // super-column internal map.
            assert columnName != null; // This should never be null for supercolumns, see decodeForSuperColumn() above
            values[clusteringSize] = columnName.equals(CompactTables.SUPER_COLUMN_MAP_COLUMN)
                                   ? collectionElement
                                   : columnName;
        }
        else
        {
            if (!metadata.isDense())
                values[clusteringSize] = columnName;
            if (collectionElement != null)
                values[clusteringSize + 1] = collectionElement;
        }

        return CompositeType.build(isStatic, values);
    }

    public static Clustering decodeClustering(CFMetaData metadata, ByteBuffer value)
    {
        int csize = metadata.comparator.size();
        if (csize == 0)
            return Clustering.EMPTY;

        if (metadata.isCompound() && CompositeType.isStaticName(value))
            return Clustering.STATIC_CLUSTERING;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(value)
                                    : Collections.singletonList(value);

        return Clustering.make(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
    }

    public static class LegacyCellName
    {
        public final Clustering clustering;
        public final ColumnDefinition column;
        public final ByteBuffer collectionElement;

        private LegacyCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
        {
            this.clustering = clustering;
            this.column = column;
            this.collectionElement = collectionElement;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < clustering.size(); i++)
                sb.append(i > 0 ? ":" : "").append(clustering.get(i) == null ? "null" : ByteBufferUtil.bytesToHex(clustering.get(i)));
            return String.format("Cellname(clustering=%s, column=%s, collElt=%s)", sb.toString(), column == null ? "null" : column.name, collectionElement == null ? "null" : ByteBufferUtil.bytesToHex(collectionElement));
        }
    }
}
