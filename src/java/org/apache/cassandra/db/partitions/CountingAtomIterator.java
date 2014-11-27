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
package org.apache.cassandra.db.partitions;

import java.io.IOException;

import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filters.DataLimits;

public class CountingAtomIterator extends WrappingAtomIterator
{
    private final DataLimits.RowCounter counter;

    public CountingAtomIterator(AtomIterator iter, DataLimits.RowCounter counter)
    {
        super(iter);
        this.counter = counter;
    }

    public DataLimits.RowCounter counter()
    {
        return counter;
    }

    @Override
    public boolean hasNext()
    {
        if (counter.isDoneForPartition())
            return false;

        return super.hasNext();
    }

    @Override
    public Atom next()
    {
        Atom atom = super.next();
        if (atom.kind() == Atom.Kind.ROW)
            counter.newRow((Row)atom);
        return atom;
    }
}
