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

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Test;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.CellPath;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.*;

public class SecondaryIndexCellSizeTest
{
    @Test
    public void test64kColumn()
    {
        // a byte buffer more than 64k
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 65);
        buffer.clear();

        //read more than 64k
        for (int i=0; i<1024*64/4 + 1; i++)
            buffer.putInt(0);

        // for read
        buffer.flip();

//        SecondaryIndexCellSizeTest.MockRowIndex mockRowIndex = new SecondaryIndexCellSizeTest.MockRowIndex();
        SecondaryIndexCellSizeTest.MockColumnIndex mockColumnIndexPrimary = new SecondaryIndexCellSizeTest.MockColumnIndex(buffer, true);
        SecondaryIndexCellSizeTest.MockColumnIndex mockColumnIndex = new SecondaryIndexCellSizeTest.MockColumnIndex(buffer, false);

        //assertTrue(mockRowIndex.validate(cell));
        boolean threw = false;
        try
        {
            mockColumnIndexPrimary.validate(null);
        }
        catch (InvalidRequestException ex)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            mockColumnIndex.validate(null, null);
        }
        catch (InvalidRequestException ex)
        {
            threw = true;
        }

        // test less than 64k value
        buffer.flip();
        buffer.clear();
        buffer.putInt(20);
        buffer.flip();

//        assertTrue(mockRowIndex.validate(cell));
        try {
            mockColumnIndexPrimary.validate(null);
        }
        catch (InvalidRequestException ex)
        {
            fail("Should not have thrown for < 64k values");
        }

        try {
            mockColumnIndex.validate(null, null);
        }
        catch (InvalidRequestException ex)
        {
            fail("Should not have thrown for < 64k values");
        }
    }

//    private class MockRowIndex extends PerRowSecondaryIndex
//    {
//        @Override
//        public void init()
//        {
//        }
//
//        @Override
//        public void validateOptions() throws ConfigurationException
//        {
//        }
//
//        @Override
//        public String getIndexName()
//        {
//            return null;
//        }
//
//        @Override
//        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
//        {
//            return null;
//        }
//
//        @Override
//        public void forceBlockingFlush()
//        {
//        }
//
//        @Override
//        public ColumnFamilyStore getIndexCfs()
//        {
//            return null;
//        }
//
//        @Override
//        public void removeIndex(ByteBuffer columnName)
//        {
//        }
//
//        @Override
//        public void invalidate()
//        {
//        }
//
//        @Override
//        public void truncateBlocking(long truncatedAt)
//        {
//        }
//
//        @Override
//        public void index(ByteBuffer rowKey, AtomIterator cf)
//        {
//        }
//
//        @Override
//        public void delete(ByteBuffer key, OpOrder.Group opGroup)
//        {
//        }
//
//        @Override
//        public void index(ByteBuffer rowKey, PartitionUpdate pu)
//        {
//        }
//
//        @Override
//        public void reload()
//        {
//        }
//
//        @Override
//        public boolean indexes(ColumnDefinition name)
//        {
//            return true;
//        }
//
//        @Override
//        public long estimateResultRows() {
//            return 0;
//        }
//    }


    private class MockColumnIndex extends AbstractSimplePerColumnSecondaryIndex
    {
        private final ByteBuffer buffer;

        MockColumnIndex(ByteBuffer buffer, boolean primaryKeyColumn)
        {
            this.buffer = buffer;
            if (primaryKeyColumn)
                columnDef = ColumnDefinition.clusteringKeyDef("foo", "bar", "bar", null, null);
            else
                columnDef = ColumnDefinition.regularDef("foo", "bar", "bar", AsciiType.instance, null);
        }

        @Override
        protected String baseKeyspace()
        {
            return "foo";
        }

        @Override
        protected String baseTable()
        {
            return "bar";
        }

        @Override
        public void init()
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
            return null;
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return null;
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
        public void delete(ByteBuffer rowKey, Clustering clustering, Cell col, OpOrder.Group opGroup, int nowInSec)
        {
        }

        @Override
        public void deleteForCleanup(ByteBuffer rowKey, Clustering clustering, Cell col, OpOrder.Group opGroup, int nowInSec) {}

        @Override
        public void insert(ByteBuffer rowKey, Clustering clustering, Cell col, OpOrder.Group opGroup, int nowInSec)
        {
        }

        @Override
        public void update(ByteBuffer rowKey, Clustering clustering, Cell oldCol, Cell col, OpOrder.Group opGroup, int nowInSec)
        {
        }

        @Override
        public void reload()
        {
        }

        @Override
        public boolean indexes(ColumnDefinition name)
        {
            return true;
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }

        @Override
        protected CBuilder buildIndexClusteringPrefix(ByteBuffer rowKey, ClusteringPrefix prefix, CellPath path)
        {
            return null;
        }

        @Override
        protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue,
                CellPath cellPath)
        {
            return buffer;
        }
    }
}
