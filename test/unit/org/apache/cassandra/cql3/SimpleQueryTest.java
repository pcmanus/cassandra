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

        assertRows(execute("SELECT * FROM %s"),
            row("first",  1, "value1"),
            row("second", 2, "value2"),
            row("third",  3, "value3")
        );
    }
}
