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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.DataInput;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LegacyLayout;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Utility class to handle deserializing atom from sstables.
 *
 * Note that this is not a full fledged AtomIterator. It's also not closeable, it is always
 * the job of the user to close the underlying ressources.
 */
public class SSTableAtomIterator extends AbstractIterator<Atom> implements Iterator<Atom>
{
    private final DataInput in;
    private final LegacyLayout.Flag flag;
    private final Descriptor.Version version;
    private final int expireBefore;

    public SSTableAtomIterator(DataInput in, LegacyLayout.Flag flag, int expireBefore, Descriptor.Version version)
    {
        this.in = in;
        this.flag = flag;
        this.version = version;
        this.expireBefore = expireBefore;
    }

    public SSTableAtomIterator(DataInput in, Descriptor.Version version)
    {
        this(in, LegacyLayout.Flag.LOCAL, Integer.MIN_VALUE, version);
    }

    protected Atom computeNext()
    {
        // TODO
        throw new UnsupportedOperationException();

        //try
        //{
        //    ...
        //}
        //catch (IOError e)
        //{
        //    if (e.getCause() instanceof IOException)
        //        throw new CorruptSSTableException((IOException)e.getCause(), filename);
        //    else
        //        throw e;
        //}
    }
}
