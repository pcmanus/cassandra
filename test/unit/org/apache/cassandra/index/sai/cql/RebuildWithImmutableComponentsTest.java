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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.impl.util.StringIgnoreCaseKeyComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.ComponentGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponentInfo;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.PathUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RebuildWithImmutableComponentsTest extends SAITester
{
    static Boolean defaultImmutableSetting;

    @BeforeClass
    public static void setProperty()
    {
        defaultImmutableSetting = IMMUTABLE_SAI_COMPONENTS.getBoolean();
        IMMUTABLE_SAI_COMPONENTS.setBoolean(true);
    }

    @AfterClass
    public static void resetProperty()
    {
        if (defaultImmutableSetting != null)
            IMMUTABLE_SAI_COMPONENTS.setBoolean(defaultImmutableSetting);
    }

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void rebuildCreateNewGenerationFiles() throws Throwable
    {
        // Setup: create index, insert data, flush, and make sure everything is correct.
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        String name = createIndex("CREATE CUSTOM INDEX test_index ON %s(val) USING 'StorageAttachedIndex'");

        IndexContext context = createIndexContext(name, UTF8Type.instance);

        execute("INSERT INTO %s (id, val) VALUES ('0', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'otherValue')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'otherValue')");

        flush();

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 'testValue'").size());

        // Rebuild the index
        rebuildIndexes(name);

        // Now, verify that:
        // 1. the new components are at generation 1
        // 2. the old components, at generation 0, still exists on disk
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.create(sstable);
            assertEquals(1, descriptor.perSSTableGroup().generation());
            assertEquals(1, descriptor.perIndexGroup(context).generation());

            Set<String> files = allSSTableFilenames(sstable);
            for (var group : List.of(descriptor.perSSTableGroup(), descriptor.perIndexGroup(context)))
            {
                for (var component : group.allComponents())
                {
                    String gen0Component = group.version().fileNameFormatter().format(component.component(), group.context(), 0);
                    String expectedFilename = sstable.descriptor.fileFor(new Component(Component.Type.CUSTOM, gen0Component)).name();
                    assertTrue( "File " + expectedFilename + " not found in " + files, files.contains(expectedFilename));
                }
            }
        }
    }

    private static Set<String> allSSTableFilenames(SSTableReader sstable)
    {
        Set<String> files = new HashSet<>();
        PathUtils.forEach(sstable.descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            if (filename.startsWith(sstable.descriptor.filenamePart()))
                files.add(filename);
        });
        return files;
    }
}
