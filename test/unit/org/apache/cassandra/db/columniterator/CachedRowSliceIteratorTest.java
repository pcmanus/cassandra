package org.apache.cassandra.db.columniterator;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.*;
import org.apache.cassandra.cache.CachedRow;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;

public class CachedRowSliceIteratorTest
{
    public static final CFMetaData CF_META_DATA = new CFMetaData("Test", "Test", ColumnFamilyType.Standard, BytesType.instance, null);

    @Test
    public void testUnboundedSlicing()
    {
        CachedRowSliceIterator iter;

        CachedRow row = getRow(10);
        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, row, null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            assertTrue(iter.hasNext());
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }
    }

    @Test
    public void testLimit()
    {
        CachedRowSliceIterator iter;
        ByteBuffer start;
        ByteBuffer end;

        start = EMPTY_BYTE_BUFFER;
        end = EMPTY_BYTE_BUFFER;

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 1; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        end = bytes(11);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 5; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 4; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }
    }

    @Test
    public void testStartAndEndRestrainedSlicing()
    {
        CachedRowSliceIterator iter;
        ByteBuffer start;
        ByteBuffer end;

        start = bytes(0);
        end = bytes(22);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(2);
        end = bytes(20);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(3);
        end = bytes(19);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 1; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 1; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(4);
        end = bytes(4);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 1; i < 2; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, false);
        for (int i = 1; i < 2; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }


        // reverse

        start = bytes(22);
        end = bytes(0);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(20);
        end = bytes(2);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(18);
        end = bytes(4);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 8; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 8; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(19);
        end = bytes(3);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 8; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 8; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        start = bytes(4);
        end = bytes(4);

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 1; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, end, true);
        for (int i = 1; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }
    }

    @Test
    public void testEndRestrainedSlicing()
    {
        CachedRowSliceIterator iter;
        ByteBuffer end;

        // end after end
        end = bytes(22);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // end is end
        end = bytes(20);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // end is other
        end = bytes(18);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // end is between
        end = bytes(19);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 9; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // end is first
        end = bytes(2);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 1; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        for (int i = 0; i < 1; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // end is before first
        end = bytes(0);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        Assert.assertFalse(iter.hasNext());

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        Assert.assertFalse(iter.hasNext());

        end = bytes(1);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        Assert.assertFalse(iter.hasNext());

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, false);
        Assert.assertFalse(iter.hasNext());


        // reversed

        // behind last
        end = bytes(0);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        //  last
        end = bytes(2);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        //  other
        end = bytes(4);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // between
        end = bytes(3);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=1; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // first
        end = bytes(20);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=9; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        for (int i = 9; i>=9; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // before first
        end = bytes(22);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        assertFalse(iter.hasNext());

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, EMPTY_BYTE_BUFFER, end, true);
        assertFalse(iter.hasNext());

    }

    @Test
    public void testStartRestrainedSlicing()
    {
        CachedRowSliceIterator iter;
        ByteBuffer start;

        // before first colum
        start = bytes(0);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // hit on first
        start = bytes(2);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // hit on other
        start = bytes(4);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 1; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 1; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // not hit
        start = bytes(3);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 1; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 1; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // start is end
        start = bytes(20);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 9; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        for (int i = 9; i < 10; i++)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }


        // start after end
        start = bytes(21);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        Assert.assertFalse(iter.hasNext());

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, false);
        Assert.assertFalse(iter.hasNext());



        // reversed

        // before first column
        start = bytes(21);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // first column
        start = bytes(20);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // other column
        start = bytes(18);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 8; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 8; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // between column
        start = bytes(19);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 8; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 8; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // start is end
        start = bytes(2);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 0; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        for (int i = 0; i>=0; i--)
        {
            IColumn next = (IColumn) iter.next();
            assertEquals(bytes(i * 2 + 2), next.name());
        }

        // start afterend
        start = bytes(0);
        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        Assert.assertFalse(iter.hasNext());

        iter = new CachedRowSliceIterator(CF_META_DATA, getRow(10), null, start, EMPTY_BYTE_BUFFER, true);
        Assert.assertFalse(iter.hasNext());
    }

    private CachedRow getRow(int numCols)
    {
        ColumnFamily columnFamily = getColumnFamily(numCols);
        return CachedRow.serialize(columnFamily);
    }

    private ColumnFamily getColumnFamily(int numCols)
    {
        ColumnFamily columns = ColumnFamily.create(CF_META_DATA);
        for (int i = 0; i < numCols; i++) {
            int num = (int) (Math.random() * 3.0);
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < num; j++) {
                builder.append(j);
            }
            ByteBuffer value = bytes(builder.toString(), Charset.forName("UTF-8"));
            columns.addColumn(QueryPath.column(bytes(i * 2 + 2)), value, timestamp());
        }
        return columns;
    }

    private long timestamp()
    {
        return System.nanoTime() / 1000;
    }
}
