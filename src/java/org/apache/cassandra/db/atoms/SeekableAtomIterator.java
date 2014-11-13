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

import org.apache.cassandra.db.ClusteringPrefix;

public interface SeekableAtomIterator extends AtomIterator
{
    /**
     * Seek forward (resp. backward if isReverseOrder() is true fro the iterator) in
     * the iterator so that the next returned Atom a is the smallest atom between
     * {@code from} and {@code to}, that is such that {@code from <= a} (resp. {@code from >= a}
     * and {@code a <= to} (resp. {@code a >= to}). This method returns true if
     * such atom exists (in which case the next call to {@code next()} should return
     * that atom) and false otherwise (in which case the state of the iterator is unknown
     * and shouldn't be relied upon until the next successful call to seekTo).
     */
    public boolean seekTo(ClusteringPrefix from, ClusteringPrefix to);
}
