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
package org.apache.cassandra.db.view;

import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Creates the updates to apply to some views given the existing rows in the base
 * table and the updates that we're applying to them (this handles updates on a
 * single partition only).
 * <p>
 * Note that this is like {@link ViewUpdateBuilder} but simply for multiple views.
 */
public class MultiViewUpdateBuilder
{
    private final CFMetaData baseTableMetadata;
    private final List<ViewUpdateBuilder> builders;
    private final int nowInSec;

    /**
     * Creates a new {@code ViewUpdateBuilder}.
     *
     * @param baseTalbeMetadata the metadata for the base table this is a builder for.
     * @param views the views for which this will be building updates for.
     * @param basePartitionKey the partition key for the base table partition for which
     * we'll handle updates for.
     * @param nowInSec the current time in seconds. Used to decide if data are live or not
     * and as base reference for new deletions.
     */
    public MultiViewUpdateBuilder(CFMetaData baseTableMetadata, Collection<View> views, DecoratedKey basePartitionKey, int nowInSec)
    {
        this.baseTableMetadata = baseTableMetadata;
        this.builders = new ArrayList<>(views.size());
        this.nowInSec = nowInSec;

        for (View view : views)
            builders.add(new ViewUpdateBuilder(view, basePartitionKey, nowInSec));
    }

    /**
     * Generates the updates to be made to the views given the existing values of a base table row
     * and the updates applied to it.
     *
     * @param existingBaseRow the base table row as it is before an update.
     * @param updateBaseRow the newly updates made to {@code existingBaseRow}.
     */
    public void generateViewsMutations(Row existingBaseRow, Row updateBaseRow)
    {
        // Having existing empty is useful, it just means we'll insert a brand new entry for updateBaseRow,
        // but if we have no update at all, we shouldn't get there.
        assert !updateBaseRow.isEmpty();

        // We allow existingBaseRow to be null, which we treat the same as being empty as an small optimization
        // to avoid allocating empty row objects when we know there was nothing existing.
        Row mergedBaseRow = existingBaseRow == null ? updateBaseRow : Rows.merge(existingBaseRow, updateBaseRow, nowInSec);
        for (ViewUpdateBuilder builder : builders)
            builder.generateViewMutations(existingBaseRow, mergedBaseRow);
    }

    /**
     * Returns the mutation that needs to be done to the views given the base table updates
     * passed to {@link #generateViewMutations}.
     *
     * @return the mutations to do to the views tracked by this builder.
     */
    public Collection<Mutation> build()
    {
        // One view is probably common enough and we can optimize a bit easily
        if (builders.size() == 1)
        {
            Collection<PartitionUpdate> updates = builders.get(0).build();
            List<Mutation> mutations = new ArrayList<>(updates.size());
            for (PartitionUpdate update : updates)
                mutations.add(new Mutation(update));
            return mutations;
        }

        Map<DecoratedKey, Mutation> mutations = new HashMap<>();
        for (ViewUpdateBuilder builder : builders)
        {
            for (PartitionUpdate update : builder.build())
            {
                DecoratedKey key = update.partitionKey();
                Mutation mutation = mutations.get(key);
                if (mutation == null)
                {
                    mutation = new Mutation(baseTableMetadata.ksName, key);
                    mutations.put(key, mutation);
                }
                mutation.add(update);
            }
        }
        return mutations.values();
    }
}
