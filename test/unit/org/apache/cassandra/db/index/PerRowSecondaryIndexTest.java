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
package org.apache.cassandra.db.index;


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonPartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.*;

public class PerRowSecondaryIndexTest
{

    // test that when index(key) is called on a PRSI index,
    // the data to be indexed can be read using the supplied
    // key. TestIndex.index(key) simply reads the data to be
    // indexed & stashes it in a static variable for inspection
    // in the test.

    private static final String KEYSPACE1 = "PerRowSecondaryIndexTest";
    private static final String CF_INDEXED = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.perRowIndexedCFMD(KEYSPACE1, CF_INDEXED));
    }

    @Before
    public void clearTestStub()
    {
        PerRowSecondaryIndexTest.TestIndex.reset();
    }

    @Test
    public void testIndexInsertAndUpdate()
    {
        // create a row then test that the configured index instance was able to read the row
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);
        ColumnDefinition cdef = cfm.getColumnDefinition(new ColumnIdentifier("indexed", true));

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k1");
        builder.add("indexed", ByteBufferUtil.bytes("foo"));
        builder.build().apply();


        AtomIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("foo"), new RowIteratorFromAtomIterator(indexedRow).next().getCell(cdef).value());

        // update the row and verify what was indexed
        builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros() + 1 , "k1");
        builder.add("indexed", ByteBufferUtil.bytes("bar"));
        builder.build().apply();

        indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("bar"), new RowIteratorFromAtomIterator(indexedRow).next().getCell(cdef).value());
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testColumnDelete()
    {
        // issue a column delete and test that the configured index instance was notified to update
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k2");
        ColumnDefinition cdef = cfm.getColumnDefinition(new ColumnIdentifier("indexed", true));


        builder.delete(cdef);
        builder.build().apply();

        AtomIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);

        //We filter tombstones now...
        Assert.assertFalse(new RowIteratorFromAtomIterator(indexedRow).hasNext());
        assertTrue(Arrays.equals("k2".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testRowDelete()
    {
        // issue a row level delete and test that the configured index instance was notified to update
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);
        RowUpdateBuilder.deleteRow(cfm, FBUtilities.timestampMicros(), "k3").apply();

        AtomIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertNotNull(indexedRow.partitionLevelDeletion());
        Assert.assertFalse(new RowIteratorFromAtomIterator(indexedRow).hasNext());
        assertTrue(Arrays.equals("k3".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testInvalidSearch()
    {

        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k1");
        builder.add("indexed", ByteBufferUtil.bytes("foo"));
        builder.build().apply();

        
        // test we can search:
        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"Indexed1\" WHERE indexed = 'foo'", KEYSPACE1));
        assertEquals(1, result.size());

        // test we can't search if the searcher doesn't validate the expression:
        try
        {
            QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"Indexed1\" WHERE indexed = 'invalid'", KEYSPACE1));
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof InvalidRequestException || (e.getCause() != null && (e.getCause() instanceof InvalidRequestException)));
        }
    }

    public static class TestIndex extends PerRowSecondaryIndex
    {
        public static AtomIterator LAST_INDEXED_ROW;
        public static ByteBuffer LAST_INDEXED_KEY;

        public static void reset()
        {
            LAST_INDEXED_KEY = null;
            LAST_INDEXED_ROW = null;
        }

        @Override
        public void index(ByteBuffer rowKey, AtomIterator cf)
        {
            LAST_INDEXED_ROW = cf;
            LAST_INDEXED_KEY = rowKey;
        }

        public void index(ByteBuffer rowKey, PartitionUpdate atoms)
        {
            LAST_INDEXED_ROW = atoms.toAtomIterator();
            LAST_INDEXED_KEY = rowKey;
        }

        @Override
        public void delete(ByteBuffer key, OpOrder.Group opGroup)
        {
        }

        @Override
        public void init()
        {
        }

        @Override
        public void reload()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
        {
            return new SecondaryIndexSearcher(baseCfs.indexManager, columns)
            {
                
                @Override
                public PartitionIterator search(ReadCommand filter)
                {
                    return new SingletonPartitionIterator(LAST_INDEXED_ROW);
                }

                @Override
                public ColumnFilter.Expression primaryClause(ReadCommand command)
                {
                    ColumnFilter.Expression expression = command.columnFilter().iterator().next();

                    if (expression.getIndexValue().equals(ByteBufferUtil.bytes("invalid")))
                        throw new InvalidRequestException("Invalid search!");

                    return expression;
                }
            };
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return baseCfs;
        }

        @Override
        public boolean indexes(ColumnDefinition name)
        {
            return true;
        }

        @Override
        public void validate(Clustering clustering) throws InvalidRequestException
        {

        }

        @Override
        public void validate(ByteBuffer cellValue, CellPath path) throws InvalidRequestException
        {

        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }
}
