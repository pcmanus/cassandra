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
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.atoms.Atom;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

public class ClusteringComparator implements Comparator<Clusterable>
{
    // TODO: this is only useful to make sure a row and a range tombstone marker with the same
    // clustering value are not equal when mergin atom iterators. If we enforce that
    // range tombstone marker clustering always have an EOC != none (which makes sense), we
    // won't need that.
    public final Comparator<Atom> atomComparator;

    public final Comparator<Row> rowComparator;

    private final List<AbstractType<?>> clusteringTypes;
    private final boolean isByteOrderComparable;

    public ClusteringComparator(List<AbstractType<?>> clusteringTypes)
    {
        this.clusteringTypes = clusteringTypes;
        this.isByteOrderComparable = isByteOrderComparable(clusteringTypes);

        // Sort first by clustering prefix. For the same clustering prefix,
        // sorts row range tombstone markers before rows.
        this.atomComparator = new Comparator<Atom>()
        {
            public int compare(Atom a1, Atom a2)
            {
                int cmp = ClusteringComparator.this.compare(a1, a2);
                if (cmp != 0)
                    return cmp;

                if (a1.kind() == Atom.Kind.ROW)
                    return a2.kind() == Atom.Kind.ROW ? 0 : 1;
                else
                    return a2.kind() == Atom.Kind.ROW ? -1 : 0;
            }
        };
        this.rowComparator = new Comparator<Row>()
        {
            public int compare(Row upd1, Row upd2)
            {
                return ClusteringComparator.this.compare(upd1, upd2);
            }
        };

    }

    private static boolean isByteOrderComparable(Iterable<AbstractType<?>> types)
    {
        boolean isByteOrderComparable = true;
        for (AbstractType<?> type : types)
            isByteOrderComparable &= type.isByteOrderComparable();
        return isByteOrderComparable;
    }

    public int size()
    {
        return clusteringTypes.size();
    }

    public AbstractType<?> subtype(int i)
    {
        return clusteringTypes.get(i);
    }

    public ClusteringPrefix make(Object... values)
    {
        if (values.length > size())
            throw new IllegalArgumentException("Too many components, max is " + size());

        CBuilder builder = new CBuilder(this);
        for (int i = 0; i < values.length; i++)
        {
            Object val = values[i];
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer)val);
            else
                builder.add(val);
        }
        return builder.build();
    }

    public ClusteringComparator setSubtype(int idx, AbstractType<?> type)
    {
        clusteringTypes.set(idx, type);
        return this;
    }

    public int compare(Clusterable cl1, Clusterable cl2)
    {
        ClusteringPrefix c1 = cl1.clustering();
        ClusteringPrefix c2 = cl1.clustering();

        if (c1 == EmptyClusteringPrefix)

        int s1 = c1.size();
        int s2 = c2.size();
        int minSize = Math.min(s1, s2);

        for (int i = 0; i < minSize; i++)
        {
            int cmp = isByteOrderComparable
                    ? ByteBufferUtil.compareUnsigned(c1.get(i), c2.get(i))
                    : clusteringTypes.get(i).compare(c1.get(i), c2.get(i));

            if (cmp != 0)
                return cmp;
        }

        if (s1 == s2)
            return c1.eoc().compareTo(c2.eoc());
        return s1 < s2 ? c1.eoc().prefixComparisonResult : -c2.eoc().prefixComparisonResult;
    }

    public boolean isCompatibleWith(ClusteringComparator previous)
    {
        if (this == previous)
            return true;

        // Extending with new components is fine, shrinking is not
        if (size() < previous.size())
            return false;

        for (int i = 0; i < previous.size(); i++)
        {
            AbstractType<?> tprev = previous.subtype(i);
            AbstractType<?> tnew = subtype(i);
            if (!tnew.isCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    public String getString(ClusteringPrefix prefix)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public Comparator<IndexInfo> indexComparator()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public List<AbstractType<?>> subtypes()
    {
        return clusteringTypes;
    }

    public Comparator<IndexInfo> indexReverseComparator()
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
