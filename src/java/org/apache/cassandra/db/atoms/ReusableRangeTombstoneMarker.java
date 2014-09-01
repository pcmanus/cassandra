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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;

public class ReusableRangeTombstoneMarker implements RangeTombstoneMarker
{
    private ClusteringPrefix clustering;
    private boolean isOpen;
    private DeletionTime delTime;

    public ReusableRangeTombstoneMarker()
    {
    }

    public ReusableRangeTombstoneMarker setTo(ClusteringPrefix clustering, boolean isOpen, DeletionTime delTime)
    {
        this.clustering = clustering;
        this.isOpen = isOpen;
        this.delTime = delTime;
        return this;
    }

    public Atom.Kind kind()
    {
        return Atom.Kind.RANGE_TOMBSTONE_MARKER;
    }

    public ClusteringPrefix clustering()
    {
        return clustering;
    }

    public boolean isOpenMarker()
    {
        return isOpen;
    }

    public DeletionTime delTime()
    {
        return delTime;
    }

    public Atom takeAlias()
    {
        return new SimpleRangeTombstoneMarker(clustering.takeAlias(), isOpen, delTime.takeAlias());
    }
}
