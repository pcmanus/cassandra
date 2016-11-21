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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQLTester;
import org.junit.Test;

public class BytesFctsTest extends CQLTester
{
    @Test
    public void testByteSizeOfFcts() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 blob, v3 text, v4 text)");

        String str = "Some string of text";
        ByteBuffer blob = ByteBuffer.allocate(42);

        execute("INSERT INTO %s(k, v1, v2, v3) VALUES (0, ?, ?, ?)", 42, str, blob);

        assertRows(execute("SELECT byteSizeOf(v1), byteSizeOf(v2), byteSizeOf(v3), byteSizeOf(v4) FROM %s"),
            row(4, str.length(), blob.remaining(), null));
    }
}

