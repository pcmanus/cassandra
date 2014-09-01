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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.singletonList;

/**
 * Builder that allow to build multiple clustering prefix at the same time.
 */
public class MultiCBuilder
{
    /**
     * The builder used to build the <code>clustering</code>s.
     */
    private final CBuilder builder;

    /**
     * The elements of the clusterings
     */
    private final List<List<ByteBuffer>> elementsList = new ArrayList<>();

    /**
     * The number of elements that still can be added.
     */
    private int remaining;

    /**
     * <code>true</code> if the clusterings have been build, <code>false</code> otherwise.
     */
    private boolean built;

    /**
     * <code>true</code> if the clusterings contains some <code>null</code> elements.
     */
    private boolean containsNull;

    public MultiCBuilder(CBuilder builder)
    {
        this.builder = builder;
        this.remaining = builder.remainingCount();
    }

    /**
     * Adds the specified element to all the clusterings.
     * <p>
     * If this builder contains 2 clustering: A-B and A-C a call to this method to add D will result in the clusterings:
     * A-B-D and A-C-D.
     * </p>
     *
     * @param value the value of the next element
     * @return this <code>MulitCBuilder</code>
     */
    public MultiCBuilder addElementToAll(ByteBuffer value)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            if (value == null)
                containsNull = true;

            elementsList.get(i).add(value);
        }
        remaining--;
        return this;
    }

    /**
     * Adds individually each of the specified elements to the end of all of the existing clusterings.
     * <p>
     * If this builder contains 2 clusterings: A-B and A-C a call to this method to add D and E will result in the 4
     * clusterings: A-B-D, A-B-E, A-C-D and A-C-E.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public MultiCBuilder addEachElementToAll(List<ByteBuffer> values)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> oldComposite = elementsList.remove(0);

            for (int j = 0, n = values.size(); j < n; j++)
            {
                List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                elementsList.add(newComposite);

                ByteBuffer value = values.get(j);

                if (value == null)
                    containsNull = true;

                newComposite.add(values.get(j));
            }
        }

        remaining--;
        return this;
    }

    /**
     * Returns the number of elements that can be added to the clusterings.
     *
     * @return the number of elements that can be added to the clusterings.
     */
    public int remainingCount()
    {
        return remaining;
    }

    /**
     * Checks if some elements can still be added to the clusterings.
     *
     * @return <code>true</code> if it is possible to add more elements to the clusterings, <code>false</code> otherwise.
     */
    public boolean hasRemaining()
    {
        return remaining > 0;
    }

    /**
     * Checks if this builder is empty.
     *
     * @return <code>true</code> if this builder is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty()
    {
        return elementsList.isEmpty();
    }

    /**
     * Checks if the clusterings contains null elements.
     *
     * @return <code>true</code> if the clusterings contains <code>null</code> elements, <code>false</code> otherwise.
     */
    public boolean containsNull()
    {
        return containsNull;
    }

    /**
     * Builds the <code>clusterings</code>.
     *
     * @return the clusterings
     */
    public List<ClusteringPrefix> build()
    {
        return build(ClusteringPrefix.EOC.NONE);
    }

    /**
     * Builds the <code>clusterings</code> with the specified EOC.
     *
     * @return the clusterings
     */
    public List<ClusteringPrefix> build(ClusteringPrefix.EOC eoc)
    {
        built = true;

        if (elementsList.isEmpty())
            return singletonList(builder.build(eoc));

        // Use a TreeSet to sort and eliminate duplicates
        Set<ClusteringPrefix> set = new TreeSet<>(builder.comparator());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> elements = elementsList.get(i);
            set.add(builder.buildWith(elements, eoc));
        }

        return new ArrayList<>(set);
    }

    private void checkUpdateable()
    {
        if (!hasRemaining() || built)
            throw new IllegalStateException("this builder cannot be updated anymore");
    }
}
