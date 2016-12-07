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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.*;

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
}
