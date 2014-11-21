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

import java.io.*;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableIdentityIterator extends SSTableAtomIterator implements Comparable<SSTableIdentityIterator>, AtomIterator
{
    private final SSTableReader sstable;
    private final DecoratedKey key;
    private final DeletionTime partitionLevelDeletion;
    private final String filename;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        super(file, LegacyLayout.Flag.LOCAL, FBUtilities.nowInSeconds(), sstable.descriptor.version);
        this.sstable = sstable;
        this.filename = file.getPath();
        this.key = key;

        try
        {
            this.partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return metadata().partitionColumns();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        return filename;
    }

    public AtomStats stats()
    {
        // TODO: we should start collecting the minTimestamp and the minTTL
        return new AtomStats(Math.max(0, sstable.sstableMetadata.minTimestamp), 0, 0);
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
