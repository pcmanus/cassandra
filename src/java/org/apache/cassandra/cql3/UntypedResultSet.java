package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.hadoop.io.UTF8;

/** a utility for doing internal cql-based queries */
public class UntypedResultSet implements Iterable<UntypedResultSet.Row>
{
    private final List<CqlRow> cqlRows;

    public UntypedResultSet(List<CqlRow> cqlRows)
    {
        this.cqlRows = cqlRows;
    }
    
    public Row one()
    {
        if (cqlRows.size() != 1)
            throw new IllegalStateException("One row required, " + cqlRows.size() + " found");
        return new Row(cqlRows.get(0));
    }

    public Iterator<Row> iterator()
    {
        return new AbstractIterator<Row>()
        {
            Iterator<CqlRow> iter = cqlRows.iterator();
            
            protected Row computeNext()
            {
                return new Row(iter.next());
            }
        };
    }

    public static class Row
    {
        Map<String, ByteBuffer> data = new HashMap<String, ByteBuffer>();

        public Row(CqlRow cqlRow)
        {
            for (Column column : cqlRow.columns)
                data.put(UTF8Type.instance.compose(column.name), column.value);
        }

        public String getString(String column)
        {
            return UTF8Type.instance.compose(data.get(column));
        }

        public boolean getBoolean(String column)
        {
            return BooleanType.instance.compose(data.get(column));
        }

        public int getInt(String column)
        {
            return Int32Type.instance.compose(data.get(column));
        }

        public double getDouble(String column)
        {
            return DoubleType.instance.compose(data.get(column));
        }

        public ByteBuffer getBytes(String column)
        {
            return data.get(column);
        }
    }
}
