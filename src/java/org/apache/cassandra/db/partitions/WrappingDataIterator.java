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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.atoms.RowIterator;

public abstract class WrappingDataIterator implements DataIterator
{
    protected final DataIterator wrapped;

    protected WrappingDataIterator(DataIterator wrapped)
    {
        this.wrapped = wrapped;
    }

    public boolean hasNext()
    {
        return wrapped.hasNext();
    }

    public RowIterator next()
    {
        return wrapped.next();
    }

    public void remove()
    {
        wrapped.remove();
    }

    public void close()
    {
        wrapped.close();
    }
}
