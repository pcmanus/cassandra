/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.PartitionRangeReadBuilder;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexTest
{
    public static final String KEYSPACE1 = "SecondaryIndexTest1";
    public static final String WITH_COMPOSITE_INDEX = "WithCompositeIndex";
    public static final String WITH_KEYS_INDEX = "WithKeysIndex";
    public static final String COMPOSITE_INDEX_TO_BE_ADDED = "CompositeIndexToBeAdded";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, WITH_COMPOSITE_INDEX, true).gcGraceSeconds(0),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, COMPOSITE_INDEX_TO_BE_ADDED, false).gcGraceSeconds(0),
                                    SchemaLoader.keysIndexCFMD(KEYSPACE1, WITH_KEYS_INDEX, true).gcGraceSeconds(0));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(COMPOSITE_INDEX_TO_BE_ADDED).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_KEYS_INDEX).truncateBlocking();
    }

    @Test
    public void testIndexScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);

        new RowUpdateBuilder(cfs.metadata, 0, "k1").clustering("c").add("birthdate", 1L).add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k2").clustering("c").add("birthdate", 2L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k3").clustering("c").add("birthdate", 1L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k4").clustering("c").add("birthdate", 3L).add("notbirthdate", 2L).build().applyUnsafe();

        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ByteBuffer nbBB = ByteBufferUtil.bytes("notbirthdate");
        ColumnDefinition bDef = cfs.metadata.getColumnDefinition(bBB);
        ColumnDefinition nbDef = cfs.metadata.getColumnDefinition(nbBB);

        // basic single-expression query
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k3"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addColumn(bBB).executeLocally())
        {
            RowIterator ri = iter.next();
            Row r = ri.next();
            // k2
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri2 = iter.next();
            r = ri2.next();
            // k3
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(1L));

            assert !ri.hasNext();
        }

        // 2 columns, 3 results
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .executeLocally())
        {
            RowIterator ri = iter.next();
            Row r = ri.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(2L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri2 = iter.next();
            r = ri2.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(1L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri3 = iter.next();
            r = ri3.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(3L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));
            assert !ri.hasNext();
        }

        // Verify getIndexSearchers finds the data for our rc
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k3"))
                .addColumn(bBB)
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(1L)).build();
        List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersFor(rc);
        assertEquals(searchers.size(), 1);
        try (UnfilteredPartitionIterator pi = searchers.get(0).search(rc))
        {
            assert(pi.hasNext());
        }

        // Verify gt on idx scan
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.GT, ByteBufferUtil.bytes(1L))
                .executeLocally())
        {
            int rowCount = 0;
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                while (ri.hasNext())
                {
                    ++rowCount;
                    assert ByteBufferUtil.toLong(ri.next().getCell(bDef).value()) > 1L;
                }
            }
            assertEquals(2, rowCount);
        }

        // Filter on non-indexed, LT comparison
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.LT, ByteBufferUtil.bytes(2L))
                .executeLocally())
        {
            assertFalse(iter.hasNext());
        }

        // Hit on primary, fail on non-indexed filter
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(1L))
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.NEQ, ByteBufferUtil.bytes(2L))
                .executeLocally())
        {
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testLargeScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ByteBuffer nbBB = ByteBufferUtil.bytes("notbirthdate");

        for (int i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata, FBUtilities.timestampMicros(), "key" + i)
                    .clustering("c")
                    .add("birthdate", 34L)
                    .add("notbirthdate", ByteBufferUtil.bytes((long) (i % 2)))
                    .build()
                    .applyUnsafe();
        }

        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(34L))
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.EQ, ByteBufferUtil.bytes(1L))
                .executeLocally())
        {
            Set<DecoratedKey> keys = new HashSet<>();
            int rowCount = 0;
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                keys.add(ri.partitionKey());
                while (ri.hasNext())
                {
                    ri.next();
                    ++rowCount;
                }
            }
            // extra check that there are no duplicate results -- see https://issues.apache.org/jira/browse/CASSANDRA-2406
            assertEquals(rowCount, keys.size());
            assertEquals(50, rowCount);
        }
    }

    @Test
    public void testCompositeIndexDeletions() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ColumnDefinition bDef = cfs.metadata.getColumnDefinition(bBB);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // Confirm addition works
        new RowUpdateBuilder(cfs.metadata, 0, "k1").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // delete the column directly
        RowUpdateBuilder.deleteRow(cfs.metadata, 1, "k1", "c").applyUnsafe();
        assertIndexedNone(cfs, col, 1L);

        // verify that it's not being indexed under any other value either
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());

        // resurrect w/ a newer timestamp
        new RowUpdateBuilder(cfs.metadata, 2, "k1").clustering("c").add("birthdate", 1L).build().apply();;
        assertIndexedOne(cfs, col, 1L);

        // verify that row and delete w/ older timestamp does nothing
        RowUpdateBuilder.deleteRow(cfs.metadata, 1, "k1", "c").applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // similarly, column delete w/ older timestamp should do nothing
        new RowUpdateBuilder(cfs.metadata, 1, "k1").clustering("c").delete(bDef).build().applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // delete the entire row (w/ newer timestamp this time)
        RowUpdateBuilder.deleteRow(cfs.metadata, 3, "k1", "c").applyUnsafe();
        rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());

        // make sure obsolete mutations don't generate an index entry
        new RowUpdateBuilder(cfs.metadata, 3, "k1").clustering("c").add("birthdate", 1L).build().apply();;
        rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());
    }

    @Test
    public void testCompositeIndexUpdate() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value, test that the index query fetches the new version
        new RowUpdateBuilder(cfs.metadata, 1, "testIndexUpdate").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 2, "testIndexUpdate").clustering("c").add("birthdate", 200L).build().applyUnsafe();

        // Confirm old version fetch fails
        assertIndexedNone(cfs, col, 100L);

        // Confirm new works
        assertIndexedOne(cfs, col, 200L);

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        assertIndexedNone(cfs, col, 300L);
        assertIndexedOne(cfs, col, 200L);
    }

    @Test
    public void testIndexUpdateOverwritingExpiringColumns() throws Exception
    {
        // see CASSANDRA-7268
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value with an expiring column
        new RowUpdateBuilder(cfs.metadata, 1L, 500, "K100").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 100L);

        // requires a 1s sleep because we calculate local expiry time as (now() / 1000) + ttl
        TimeUnit.SECONDS.sleep(1);

        // now overwrite with the same name/value/ttl, but the local expiry time will be different
        new RowUpdateBuilder(cfs.metadata, 1L, 500, "K100").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 100L);

        // check that modifying the indexed value using the same timestamp behaves as expected
        new RowUpdateBuilder(cfs.metadata, 1L, 500, "K101").clustering("c").add("birthdate", 101L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 101L);

        TimeUnit.SECONDS.sleep(1);

        new RowUpdateBuilder(cfs.metadata, 1L, 500, "K101").clustering("c").add("birthdate", 102L).build().applyUnsafe();
        // Confirm 101 is gone
        assertIndexedNone(cfs, col, 101L);

        // Confirm 102 is there
        assertIndexedOne(cfs, col, 102L);
    }

    @Test
    public void testDeleteOfInconsistentValuesInKeysIndex() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_KEYS_INDEX);

        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the "birthdate" value
        new RowUpdateBuilder(cfs.metadata, 1, "k1").noRowMarker().clustering("c").add("birthdate", 1L).build().applyUnsafe();

        // force a flush, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(WITH_KEYS_INDEX).forceBlockingFlush();

        // now apply another update, but force the index update to be skipped
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 2, "k1").noRowMarker().clustering("c").add("birthdate", 2L).build(), true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, 1L);
        assertIndexedNone(cfs, col, 2L);

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 3, "k1").noRowMarker().clustering("c").add("birthdate", 1L).build(), true, false);
        assertIndexedNone(cfs, col, 1L);
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndex() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = WITH_COMPOSITE_INDEX;

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the author value
        new RowUpdateBuilder(cfs.metadata, 0, "k1").clustering("c").add("birthdate", 10l).build().applyUnsafe();

        // test that the index query fetches this version
        assertIndexedOne(cfs, col, 10l);

        // force a flush and retry the query, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(cfName).forceBlockingFlush();
        assertIndexedOne(cfs, col, 10l);

        // now apply another update, but force the index update to be skipped
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 1, "k1").clustering("c").add("birthdate", 20l).build(), true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, 10l);
        assertIndexedNone(cfs, col, 20l);

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        // TODO: Figure out why this is re-inserting
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 2, "k1").clustering("c1").add("birthdate", 10l).build(), true, false);
        assertIndexedNone(cfs, col, 20l);
    }

    // See CASSANDRA-6098
    @Test
    public void testDeleteCompositeIndex() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);

        ByteBuffer colName = ByteBufferUtil.bytes("birthdate");

        // Insert indexed value.
        new RowUpdateBuilder(cfs.metadata, 1, "k1").clustering("c").add("birthdate", 10l).build().applyUnsafe();

        // Now delete the value
        RowUpdateBuilder.deleteRow(cfs.metadata, 2, "k1", "c").applyUnsafe();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        assertIndexedNone(cfs, colName, 10l);
    }

    @Test
    public void testDeleteKeysIndex() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_KEYS_INDEX);

        ByteBuffer colName = ByteBufferUtil.bytes("birthdate");

        // Insert indexed value.
        new RowUpdateBuilder(cfs.metadata, 1, "k1").clustering("c").add("birthdate", 10l).build().applyUnsafe();

        // Now delete the value
        RowUpdateBuilder.deleteRow(cfs.metadata, 2, "k1", "c").applyUnsafe();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        assertIndexedNone(cfs, colName, 10l);
    }

    // See CASSANDRA-2628
    @Test
    public void testIndexScanWithLimitOne()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        Mutation rm;

        new RowUpdateBuilder(cfs.metadata, 0, "kk1").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk1").clustering("c").add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk2").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk2").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk3").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk3").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk4").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk4").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();

        // basic single-expression query, limit 1
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
             .addFilter(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate")), Operator.EQ, ByteBufferUtil.bytes(1L))
             .addFilter(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("notbirthdate")), Operator.GT, ByteBufferUtil.bytes(1L))
             .setCQLLimit(1)
             .executeLocally())
        {
            int legitRows = 0;
            // We get back a RowIterator for each partitionKey but all but 1 should be empty
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                while (ri.hasNext())
                {
                    ++legitRows;
                    ri.next();
                }
            }
            assertEquals(1, legitRows);
        }
    }

    @Test
    public void testIndexCreate() throws IOException, InterruptedException, ExecutionException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COMPOSITE_INDEX_TO_BE_ADDED);

        // create a row and update the birthdate value, test that the index query fetches the new version
        DecoratedKey dk = Util.dk("k1");
        new RowUpdateBuilder(cfs.metadata, 0, "k1").clustering("c").add("birthdate", 1L).build().applyUnsafe();

        ColumnDefinition old = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate"));
        old.setIndex("birthdate_index", IndexType.COMPOSITES, Collections.EMPTY_MAP);
        Future<?> future = cfs.indexManager.addIndexedColumn(old);
        future.get();
        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate"));
        assert cfs.indexManager.getIndexForColumn(cDef).getIndexCfs().getSSTables().size() > 0;

        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), 1L);

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        SecondaryIndex indexedCfs = cfs.indexManager.getIndexForColumn(cDef);
        cfs.indexManager.removeIndexedColumn(ByteBufferUtil.bytes("birthdate"));
        assert !indexedCfs.isIndexBuilt(ByteBufferUtil.bytes("birthdate"));

        // rebuild & re-query
        future = cfs.indexManager.addIndexedColumn(cDef);
        future.get();
        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), 1L);
    }

    @Test
    public void testKeysSearcherSimple() throws Exception
    {
        //  Create secondary index and flush to disk
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_KEYS_INDEX);

        for (int i = 0; i < 10; i++)
            new RowUpdateBuilder(cfs.metadata, 0, "k" + i).noRowMarker().clustering("c").add("birthdate", 1l).build().applyUnsafe();

        cfs.forceBlockingFlush();
        assertIndexedCount(cfs, ByteBufferUtil.bytes("birthdate"), 1l, 10);
    }

    private void assertIndexedNone(ColumnFamilyStore cfs, ByteBuffer col, Object val)
    {
        assertIndexedCount(cfs, col, val, 0);
    }
    private void assertIndexedOne(ColumnFamilyStore cfs, ByteBuffer col, Object val)
    {
        assertIndexedCount(cfs, col, val, 1);
    }
    private void assertIndexedCount(ColumnFamilyStore cfs, ByteBuffer col, Object val, int count)
    {
        ColumnDefinition cdef = cfs.metadata.getColumnDefinition(col);
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cdef, Operator.EQ, ((AbstractType) cdef.cellValueType()).decompose(val))
                .build();

        List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersFor(rc);
        if (count != 0)
            assertTrue(searchers.size() > 0);

        try (UnfilteredPartitionIterator iter = searchers.get(0).search(rc))
        {
            assertEquals(count, Iterators.size(iter));
        }
    }
    private void assertRangeCount(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val, int count)
    {
        assertRangeCount(cfs, cfs.metadata.getColumnDefinition(col), val, count);
    }
    private void assertRangeCount(ColumnFamilyStore cfs, ColumnDefinition col, ByteBuffer val, int count)
    {
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addFilter(col, Operator.EQ, val)
                .build().executeLocally())
        {
            int found = 0;
            if (count != 0)
            {
                while (iter.hasNext())
                {
                    RowIterator ri = iter.next();
                    while (ri.hasNext())
                    {
                        Row r = ri.next();
                        if (r.getCell(col).value().equals(val))
                            ++found;
                    }
                }
            }
            assertEquals(count, found);
        }
    }
}
