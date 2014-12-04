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
package org.apache.cassandra.cql3;

import org.junit.Test;

import static junit.framework.Assert.*;

public class SimpleQueryTest extends CQLTester
{
    @Test
    public void testTableWithoutClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v1 int, v2 text);");

        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3");

        flush();

        assertRows(execute("SELECT * FROM %s WHERE k = ?", "first"),
            row("first", 1, "value1")
        );

        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
            row("value2")
        );

        assertRows(execute("SELECT * FROM %s"),
            row("first",  1, "value1"),
            row("second", 2, "value2"),
            row("third",  3, "value3")
        );
    }

    @Test
    public void testTableWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v1 text, v2 text, PRIMARY KEY (k, t));");

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 1, "v11", "v21");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 2, "v12", "v22");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 3, "v13", "v23");

        flush();

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 4, "v14", "v24");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 5, "v15", "v25");

        assertRows(execute("SELECT * FROM %s"),
            row("key",  1, "v11", "v21"),
            row("key",  2, "v12", "v22"),
            row("key",  3, "v13", "v23"),
            row("key",  4, "v14", "v24"),
            row("key",  5, "v15", "v25")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
            row("key",  4, "v14", "v24"),
            row("key",  5, "v15", "v25")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
            row("key",  2, "v12", "v22"),
            row("key",  3, "v13", "v23")
        );
    }
}
