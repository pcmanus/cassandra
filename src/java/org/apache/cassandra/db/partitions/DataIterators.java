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

import java.util.*;
import java.security.MessageDigest;

import org.apache.cassandra.db.atoms.*;

public abstract class DataIterators
{
    private DataIterators() {}

    public static final DataIterator EMPTY = new DataIterator()
    {
        public boolean hasNext()
        {
            return false;
        }

        public RowIterator next()
        {
            throw new NoSuchElementException();
        }

        public void remove()
        {
        }

        public void close()
        {
        }
    };

    public static RowIterator getOnlyElement(DataIterator iter)
    {
        // TODO (should wrap the single returned element so that the
        // close method actually close the DataIterator in argument, but
        // could avoid that if we know of the DataIterator in argument is
        // of a class that don't need closing)
        throw new UnsupportedOperationException();
    }

    public static DataIterator concat(List<DataIterator> iterators)
    {
        // TODO 
        throw new UnsupportedOperationException();
    }

    public static void digest(DataIterator iterator, MessageDigest digest)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static DataIterator singletonIterator(RowIterator iterator)
    {
        // TODO
        throw new UnsupportedOperationException();
    }
}
