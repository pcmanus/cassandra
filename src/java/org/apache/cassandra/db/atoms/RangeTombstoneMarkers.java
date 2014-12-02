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

    public static class Merger
    {
        private final AtomIterator.MergeListener listener;
        private final DeletionTime partitionDeletion;

        private ClusteringPrefix clustering;
        private final RangeTombstoneMarker[] markers;

        // Stores for each iterator, what is the currently open marker
        private final DeletionTimeArray openMarkers;
        private final DeletionTimeArray.Cursor openMarkersCursor;

        // The index in openMarkers of the "biggest" marker. This is the last open marker
        // that has been returned for the merge.
        private int openMarker = -1;

        // As reusable marker to return the result
        private final ReusableRangeTombstoneMarker reusableMarker;

        public Merger(int size, DeletionTime partitionDeletion, AtomIterator.MergeListener listener)
        {
            this.listener = listener;
            this.partitionDeletion = partitionDeletion;

            this.markers = new RangeTombstoneMarker[size];
            this.openMarkers = new DeletionTimeArray(size);
            this.openMarkersCursor = openMarkers.newCursor();

            this.reusableMarker = new ReusableRangeTombstoneMarker();
        }

        public void clear()
        {
            Arrays.fill(markers, null);
        }

        public void addMarker(int i, RangeTombstoneMarker marker)
        {
            clustering = marker.clustering();
            markers[i] = marker;
        }

        public RangeTombstoneMarker merge()
        {
            int toReturn = -1;
            boolean hasCloseMarker = false;

            for (int i = 0; i < size; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // We can completely ignore any marker that is shadowed by a partition level
                // deletion
                if (partitionDeletion.supersedes(marker.delTime()))
                    continue;

                // It's slightly easier to deal with close marker as a 2nd step
                if (!marker.isOpenMarker())
                {
                    hasCloseMarker = true;
                    continue;
                }

                // We have an open marker. It's only present after merge if it's bigger than the
                // currently open marker.
                DeletionTime dt = marker.delTime();
                openMarkers.set(i, dt);

                if (!openMarkers.supersedes(openMarker, dt))
                    openMarker = toReturn = i;
            }

            if (hasCloseMarker)
            {
                for (int i = 0; i < size; i++)
                {
                    RangeTombstoneMarker marker = markers[i];
                    if (marker == null || marker.isOpenMarker())
                        continue;

                    // Close the marker for this iterator
                    openMarkers.clear(i);

                    // What we do now depends on what the current open marker is. If it's not i, then we can
                    // just ignore that close (the corresponding open marker has been ignored or replace).
                    // If it's i, then we need to find the next biggest open marker. If there is none, then
                    // we're closing the only open marker and so we return this close marker. Otherwise, that
                    // new biggest marker is the new open one and we should return it.
                    if (i == openMarker)
                    {
                        // We've cleaned i so updateOpenMarker will return the new biggest one
                        updateOpenMarker();
                        if (openMarker < 0)
                        {
                            // We've closed the last open marker so not only should we return
                            // this close marker, but we're done with the iteration here
                            if (listener != null)
                                listener.onMergedRangeTombstoneMarkers(clustering, false, marker.delTime(), markers);

                            return reusableMarker.setTo(clustering, false, marker.delTime());
                        }

                        // Note that if toReturn is set at the beginning of this loop, it's necessarily equal
                        // to openMarker. So if we've closed the previous biggest open marker, it's ok to
                        // also update toReturn
                        toReturn = openMarker;
                    }
                }
            }

            if (toReturn < 0)
                return null;

            // Note that we can only arrive here if we have an open marker to return
            openMarkersCursor.setTo(toReturn);
            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(clustering, true, openMarkersCursor, markers);
            return reusableMarker.setTo(clustering, true, openMarkersCursor);
        }

        public DeletionTime activeDeletion()
        {
            // Note that we'll only have an openMarker if it supersedes the partition deletion
            return openMarker < 0 ? partitionDeletion : openMarkersCursor.setTo(openMarker);
        }

        private void updateOpenMarker()
        {
            openMarker = -1;
            for (int i = 0; i < openMarkers.size(); i++)
            {
                if (openMarkers.isLive(i) && (openMarker < 0 || openMarkers.supersedes(i, openMarker)))
                    openMarker = i;
            }
        }
    }
}
