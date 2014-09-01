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
    private final CFMetaData metadata;
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
        this(sstable.metadata, file, file.getPath(), key, sstable, LegacyLayout.Flag.LOCAL);
    }

    // sstable may be null, in which case we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    SSTableReader sstable,
                                    LegacyLayout.Flag flag)
    {
        super(in, flag, FBUtilities.nowInSeconds(), sstable == null ? Descriptor.Version.CURRENT : sstable.descriptor.version);
        this.metadata = metadata;
        this.filename = filename;
        this.key = key;

        try
        {
            this.partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public Columns columns()
    {
        return metadata.regularColumns();
    }

    public Columns staticColumns()
    {
        return metadata.staticColumns();
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

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
