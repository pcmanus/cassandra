package org.apache.cassandra.cache;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author smeet
 */
public class CacheRowSerializerTest
{

    public static final CFMetaData CF_META_DATA = new CFMetaData("Test", "Test", ColumnFamilyType.Standard, BytesType.instance, null);

    @Test
    public void testBinarySearch()
    {
        ColumnFamily columnFamily = getColumnFamily(4);
        ByteBuffer row = CachedRowSerializer.serialize(columnFamily);

        int i = CachedRowSerializer.binarySearch(row, ByteBufferUtil.bytes(3), BytesType.instance);
        assertEquals(3, i);

        IColumn column = CachedRowSerializer.deserializeColumn(row, ByteBufferUtil.bytes(3), BytesType.instance);
        assertNotNull(column);
        assertEquals(ByteBufferUtil.bytes(3), column.name());
    }

    @Test
    public void testAppendRow()
    {
        ColumnFamily columnFamily = getColumnFamily(1);
        ByteBuffer row = CachedRowSerializer.serialize(columnFamily);

        ColumnFamily columns = ColumnFamily.create(CF_META_DATA);
        CachedRowSerializer.appendRow(row, columns, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, 0, false, BytesType.instance);
        
        assertEquals(1, columns.getColumnCount());
        
    }

    private ColumnFamily getColumnFamily(int length)
    {
        ColumnFamily columns = ColumnFamily.create(CF_META_DATA);
        for (int i = 0; i < length; i++)
            columns.addColumn(QueryPath.column(ByteBufferUtil.bytes(i)), ByteBufferUtil.bytes(0), timestamp());
        return columns;
    }

    private long timestamp()
    {
        return System.nanoTime() / 1000;
    }
}
