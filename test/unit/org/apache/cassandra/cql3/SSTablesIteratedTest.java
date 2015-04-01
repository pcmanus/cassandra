package org.apache.cassandra.cql3;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClearableHistogram;

/**
 * Tests for checking how many sstables we access during cql queries with LIMIT specified,
 * see CASSANDRA-8180.
 */
public class SSTablesIteratedTest extends CQLTester
{
    private void executeAndCheck(String query, int numSSTables, Object[]... rows) throws Throwable
    {
        ((ClearableHistogram) cfs().metric.sstablesPerReadHistogram.cf).clear(); // resets counts

        assertRows(execute(query), rows);

       // assertEquals(numSSTables, cfs().metric.sstablesPerReadHistogram.cf.getSnapshot().getMax()); // max sstables read
    }

    @Test
    public void testSSTablesOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 3, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 30, "30"));
    }

    @Test
    public void testMixedMemtableSStables() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30")); // 1 because of min / max clustering are empty
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 30, "30")); // 1 because of min / max clustering are empt
    }

    @Test
    public void testOverlappingSStables() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 30, "30"));
    }

    @Test
    public void testDeletionOnDifferentSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("DELETE FROM %s WHERE id=1 and col=30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 3, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 4, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 4, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 4, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 2);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 3, row(1, 20, "20"));
    }

    @Test
    public void testDeletionOnSameSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        execute("DELETE FROM %s WHERE id=1 and col=30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 2, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 3, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 3, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 2, row(1, 20, "20"));
    }

    @Test
    public void testDeletionOnMemTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        execute("DELETE FROM %s WHERE id=1 and col=30");

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 20, "20"));
    }

    //TODO - test sstables larger than 64k (config.column_index_size_in_kb) so that they are indexed and we can
    //read the deletion info from the index row

    @Test
    public void testDeletionOnIndexedSSTable() throws Throwable
    {
        // reduce the column index size so that columns get indexed during flush
        DatabaseDescriptor.setColumnIndexSize(1);

        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        for (int i = 1; i <= 1000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
        }
        flush();

        Object[][] allRows = new Object[1000][];
        for (int i = 1001; i <= 2000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            allRows[2000 - i] = row(1, i, Integer.toString(i));
        }

        for (int i = 1; i <= 1000; i++)
        {
            execute("DELETE FROM %s WHERE id=1 and col = ?", i);
        }
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 2000, "2000"), row(1, 1999, "1999"));

        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000", 1, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000", 1, allRows);
    }

}