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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 *  A Cell Iterator over SSTable
 */
public class SSTableSliceIterator implements AtomIterator
{
    protected final SSTableReader sstable;
    protected final DecoratedKey key;
    protected final Columns columns;
    protected final Columns staticColumns;
    protected final Slices slices;
    protected final boolean reversed;

    // TODO
    //private final OnDiskAtomIterator reader;

    public SSTableSliceIterator(SSTableReader sstable, DecoratedKey key, Columns columns, Columns staticColumns, Slices slices, boolean reversed)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columns;
        this.staticColumns = staticColumns;
        this.slices = slices;
        this.reversed = reversed;

        // TODO
        //RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
        //this.reader = indexEntry == null ? null : createReader(sstable, indexEntry, null, slices, reversed);
    }

    /**
     * An iterator for a slice within an SSTable
     * @param sstable Keyspace for the CFS we are reading from
     * @param file Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under
     * @param slices the column slices
     * @param reversed Results are returned in reverse order iff reversed is true.
     * @param indexEntry position of the row
     */
    public SSTableSliceIterator(SSTableReader sstable,
                                FileDataInput file,
                                DecoratedKey key,
                                Columns columns,
                                Columns staticColumns,
                                Slices slices,
                                boolean reversed,
                                RowIndexEntry indexEntry)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columns;
        this.staticColumns = staticColumns;
        this.slices = slices;
        this.reversed = reversed;

        // TODO
        //reader = createReader(sstable, indexEntry, file, slices, reversed);
    }

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public Columns columns()
    {
        return columns;
    }

    public Columns staticColumns()
    {
        return staticColumns;
    }

    public boolean isReverseOrder()
    {
        return reversed;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public Row staticRow()
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    //private static OnDiskAtomIterator createReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput file, ColumnSlice[] slices, boolean reversed)
    //{
    //    return slices.length == 1 && slices[0].start.isEmpty() && !reversed
    //         ? new SimpleSliceReader(sstable, indexEntry, file, slices[0].finish)
    //         : new IndexedSliceReader(sstable, indexEntry, file, slices, reversed);
    //}

    //public ColumnFamily getColumnFamily()
    //{
    //    return reader == null ? null : reader.getColumnFamily();
    //}

    public boolean hasNext()
    {
        // TODO
        throw new UnsupportedOperationException();
        //return reader != null && reader.hasNext();
    }

    public Atom next()
    {
        // TODO
        throw new UnsupportedOperationException();
        // return reader.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //if (reader != null)
        //    reader.close();
    }

}
