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
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

/**
 * Static utilities to work on RangeTombstoneMarker objects.
 */
public abstract class RangeTombstoneMarkers
{
    private RangeTombstoneMarkers() {}

    public static String toString(CFMetaData metadata, RangeTombstoneMarker marker)
    {
        StringBuilder sb = new StringBuilder();
        ClusteringPrefix clustering = marker.clustering();
        sb.append("Marker");
        sb.append(": ");
        for (int i = 0; i < clustering.size(); i++)
        {
            if (i > 0)
                sb.append(", ");
            ColumnDefinition c = metadata.clusteringColumns().get(i);
            sb.append(c.name).append("=").append(c.type.getString(clustering.get(i)));
        }
        sb.append(" - ").append(marker.isOpenMarker() ? "open" : "close");
        sb.append("@").append(marker.delTime().markedForDeleteAt());
        return sb.toString();
    }
}
