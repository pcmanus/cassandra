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

/**
 * A marker for a range tombsone.
 *
 * There is 2 types of marker: opening and closing one. Each marker also has
 * an associated deletion times.
 *
 * Note that during iteration, their maybe 2 subsequent opening marker without
 * a closing one before the opening. In this case, the newly opened marker
 * simply indicate that new deletion times take precedence over the previously
 * opened ones.
 */
public interface RangeTombstoneMarker extends Atom
{
    public boolean isOpenMarker();
    public DeletionTime delTime();

    public interface Writer
    {
        public void writeMarker(ClusteringPrefix clustering, boolean isOpenMarker, DeletionTime delTime);
    }
}
