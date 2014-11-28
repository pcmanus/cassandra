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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.UTMetaData;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SCHEMA_{KEYSPACES, COLUMNFAMILIES, COLUMNS}_CF are used to store Keyspace/ColumnFamily attributes to make schema
 * load/distribution easy, it replaces old mechanism when local migrations where serialized, stored in system.Migrations
 * and used for schema distribution.
 */
public class DefsTables
{
    private static final Logger logger = LoggerFactory.getLogger(DefsTables.class);

    /**
     * Load keyspace definitions for the system keyspace (system.SCHEMA_KEYSPACES_CF)
     *
     * @return Collection of found keyspace definitions
     */
    public static Collection<KSMetaData> loadFromKeyspace()
    {
        List<KSMetaData> keyspaces = new ArrayList<KSMetaData>();
        try (DataIterator serializedSchema = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF))
        {
            while (serializedSchema.hasNext())
            {
                RowIterator partition = serializedSchema.next();
                if (Schema.invalidSchemaPartition(partition) || Schema.ignoredSchemaPartition(partition))
                    continue;

                keyspaces.add(KSMetaData.fromSchema(partition,
                            SystemKeyspace.readSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, partition.partitionKey()),
                            SystemKeyspace.readSchema(SystemKeyspace.SCHEMA_USER_TYPES_CF, partition.partitionKey())));
            }

            return keyspaces;
        }
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchemaInternal(mutations, true);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchemaInternal(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key().getKey()));


        // current state of the schema
        Map<DecoratedKey, RowIterator> oldKeyspaces = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> oldColumnFamilies = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> oldTypes = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_USER_TYPES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> oldFunctions = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_CF, keyspaces);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush && !StorageService.instance.isClientMode())
            flushSchemaCFs();

        // with new data applied
        Map<DecoratedKey, RowIterator> newKeyspaces = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> newColumnFamilies = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> newTypes = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_USER_TYPES_CF, keyspaces);
        Map<DecoratedKey, RowIterator> newFunctions = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_CF, keyspaces);

        Set<DecoratedKey> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeColumnFamilies(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (DecoratedKey keyspaceToDrop : keyspacesToDrop)
            dropKeyspace(keyspaceToDrop);
    }

    private static Set<DecoratedKey> mergeKeyspaces(Map<DecoratedKey, RowIterator> old, Map<DecoratedKey, RowIterator> updated)
    {
        for (RowIterator upd : updated.values())
        {
            try (RowIterator newPartition = upd; RowIterator oldPartition = old.remove(newPartition.partitionKey()))
            {
                if (oldPartition == null || RowIterators.isEmpty(oldPartition))
                    addKeyspace(KSMetaData.fromSchema(newPartition, Collections.<CFMetaData>emptyList(), new UTMetaData()));
                else
                    updateKeyspace(KSMetaData.fromSchema(newPartition, Collections.<CFMetaData>emptyList(), new UTMetaData()));
            }
        }

        // What's remain in old is those keyspace that are not in updated, i.e. the dropped ones.
        FileUtils.closeQuietly(old.values());
        return old.keySet();
    }

    private static void mergeColumnFamilies(Map<DecoratedKey, RowIterator> old, Map<DecoratedKey, RowIterator> updated)
    {
        for (RowIterator upd : updated.values())
        {
            try (RowIterator newPartition = upd; RowIterator oldPartition = old.remove(newPartition.partitionKey()))
            {
                if (oldPartition == null || RowIterators.isEmpty(oldPartition))
                {
                    // Means a new keyspace was added. Add all of it's new tables
                    for (CFMetaData cfDef : KSMetaData.deserializeColumnFamilies(newPartition).values())
                        addColumnFamily(cfDef);
                    continue;
                }

                final CFMetaData metadata = newPartition.metadata();
                final DecoratedKey key = newPartition.partitionKey();

                diffSchema(oldPartition, newPartition, new Differ()
                {
                    public void onDropped(Row oldRow)
                    {
                        CFMetaData dropped = CFMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow));
                        dropColumnFamily(dropped.ksName, dropped.cfName);
                    }

                    public void onAdded(Row newRow)
                    {
                        addColumnFamily(CFMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }

                    public void onUpdated(Row oldRow, Row newRow)
                    {
                        updateColumnFamily(CFMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }
                });
            }
        }
    }

    private static void mergeTypes(Map<DecoratedKey, RowIterator> old, Map<DecoratedKey, RowIterator> updated)
    {
        for (RowIterator upd : updated.values())
        {
            try (RowIterator newPartition = upd; RowIterator oldPartition = old.remove(newPartition.partitionKey()))
            {
                if (oldPartition == null || RowIterators.isEmpty(oldPartition))
                {
                    // Means a new keyspace was added. Add all of it's new types
                    for (UserType ut : UTMetaData.fromSchema(newPartition).values())
                        addType(ut);
                    continue;
                }

                final CFMetaData metadata = newPartition.metadata();
                final DecoratedKey key = newPartition.partitionKey();

                diffSchema(oldPartition, newPartition, new Differ()
                {
                    public void onDropped(Row oldRow)
                    {
                        dropType(UTMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow)));
                    }

                    public void onAdded(Row newRow)
                    {
                        addType(UTMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }

                    public void onUpdated(Row oldRow, Row newRow)
                    {
                        updateType(UTMetaData.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }
                });
            }
        }
    }

    private static void mergeFunctions(Map<DecoratedKey, RowIterator> old, Map<DecoratedKey, RowIterator> updated)
    {
        for (RowIterator upd : updated.values())
        {
            try (RowIterator newPartition = upd; RowIterator oldPartition = old.remove(newPartition.partitionKey()))
            {
                if (oldPartition == null || RowIterators.isEmpty(oldPartition))
                {
                    // Means a new keyspace was added. Add all of it's new types
                    for (UDFunction udf : UDFunction.fromSchema(newPartition).values())
                        addFunction(udf);
                    continue;
                }

                final CFMetaData metadata = newPartition.metadata();
                final DecoratedKey key = newPartition.partitionKey();

                diffSchema(oldPartition, newPartition, new Differ()
                {
                    public void onDropped(Row oldRow)
                    {
                        dropFunction(UDFunction.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow)));
                    }

                    public void onAdded(Row newRow)
                    {
                        addFunction(UDFunction.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }

                    public void onUpdated(Row oldRow, Row newRow)
                    {
                        updateFunction(UDFunction.fromSchema(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow)));
                    }
                });
            }
        }
    }

    private static void addKeyspace(KSMetaData ksm)
    {
        assert Schema.instance.getKSMetaData(ksm.name) == null;
        Schema.instance.load(ksm);

        if (!StorageService.instance.isClientMode())
        {
            Keyspace.open(ksm.name);
            MigrationManager.instance.notifyCreateKeyspace(ksm);
        }
    }

    private static void addColumnFamily(CFMetaData cfm)
    {
        assert Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName) == null;
        KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);
        ksm = KSMetaData.cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));

        logger.info("Loading {}", cfm);

        Schema.instance.load(cfm);

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        Keyspace.open(cfm.ksName);

        Schema.instance.setKeyspaceDefinition(ksm);

        if (!StorageService.instance.isClientMode())
        {
            Keyspace.open(ksm.name).initCf(cfm.cfId, cfm.cfName, true);
            MigrationManager.instance.notifyCreateColumnFamily(cfm);
        }
    }

    private static void addType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Loading {}", ut);

        ksm.userTypes.addType(ut);

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyCreateUserType(ut);
    }

    private static void addFunction(UDFunction udf)
    {
        logger.info("Loading {}", udf);

        Functions.addFunction(udf);

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyCreateFunction(udf);
    }

    private static void updateKeyspace(KSMetaData newState)
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(newState.name);
        assert oldKsm != null;
        KSMetaData newKsm = KSMetaData.cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        Schema.instance.setKeyspaceDefinition(newKsm);

        if (!StorageService.instance.isClientMode())
        {
            Keyspace.open(newState.name).createReplicationStrategy(newKsm);
            MigrationManager.instance.notifyUpdateKeyspace(newKsm);
        }
    }

    private static void updateColumnFamily(CFMetaData newState)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(newState.ksName, newState.cfName);
        assert cfm != null;
        cfm.reload();

        if (!StorageService.instance.isClientMode())
        {
            Keyspace keyspace = Keyspace.open(cfm.ksName);
            keyspace.getColumnFamilyStore(cfm.cfName).reload();
            MigrationManager.instance.notifyUpdateColumnFamily(cfm);
        }
    }

    private static void updateType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Updating {}", ut);

        ksm.userTypes.addType(ut);

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyUpdateUserType(ut);
    }

    private static void updateFunction(UDFunction udf)
    {
        logger.info("Updating {}", udf);

        Functions.replaceFunction(udf);

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyUpdateFunction(udf);
    }

    private static void dropKeyspace(DecoratedKey dk)
    {
        String ksName = AsciiType.instance.compose(dk.getKey());
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        String snapshotName = Keyspace.getTimestampedSnapshotName(ksName);

        CompactionManager.instance.interruptCompactionFor(ksm.cfMetaData().values(), true);

        Keyspace keyspace = Keyspace.open(ksm.name);

        // remove all cfs from the keyspace instance.
        List<UUID> droppedCfs = new ArrayList<>();
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfm.cfName);

            Schema.instance.purge(cfm);

            if (!StorageService.instance.isClientMode())
            {
                if (DatabaseDescriptor.isAutoSnapshot())
                    cfs.snapshot(snapshotName);
                Keyspace.open(ksm.name).dropCf(cfm.cfId);
            }

            droppedCfs.add(cfm.cfId);
        }

        // remove the keyspace from the static instances.
        Keyspace.clear(ksm.name);
        Schema.instance.clearKeyspaceDefinition(ksm);

        keyspace.writeOrder.awaitNewBarrier();

        // force a new segment in the CL
        CommitLog.instance.forceRecycleAllSegments(droppedCfs);

        if (!StorageService.instance.isClientMode())
        {
            MigrationManager.instance.notifyDropKeyspace(ksm);
        }
    }

    private static void dropColumnFamily(String ksName, String cfName)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        assert ksm != null;
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);
        assert cfs != null;

        // reinitialize the keyspace.
        CFMetaData cfm = ksm.cfMetaData().get(cfName);

        Schema.instance.purge(cfm);
        Schema.instance.setKeyspaceDefinition(makeNewKeyspaceDefinition(ksm, cfm));

        CompactionManager.instance.interruptCompactionFor(Arrays.asList(cfm), true);

        if (!StorageService.instance.isClientMode())
        {
            if (DatabaseDescriptor.isAutoSnapshot())
                cfs.snapshot(Keyspace.getTimestampedSnapshotName(cfs.name));
            Keyspace.open(ksm.name).dropCf(cfm.cfId);
            MigrationManager.instance.notifyDropColumnFamily(cfm);

            CommitLog.instance.forceRecycleAllSegments(Collections.singleton(cfm.cfId));
        }
    }

    private static void dropType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        ksm.userTypes.removeType(ut);

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyDropUserType(ut);
    }

    private static void dropFunction(UDFunction udf)
    {
        logger.info("Drop {}", udf);

        // TODO: this is kind of broken as this remove all overloads of the function name
        Functions.removeFunction(udf.name(), udf.argTypes());

        if (!StorageService.instance.isClientMode())
            MigrationManager.instance.notifyDropFunction(udf);
    }

    private static KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return KSMetaData.cloneWith(ksm, newCfs);
    }

    private static void flushSchemaCFs()
    {
        for (String cf : SystemKeyspace.allSchemaCfs)
            SystemKeyspace.forceBlockingFlush(cf);
    }

    public interface Differ
    {
        public void onDropped(Row oldRow);
        public void onAdded(Row newRow);
        public void onUpdated(Row oldRow, Row newRow);
    }

    private static void diffSchema(RowIterator oldPartition, RowIterator newPartition, Differ differ)
    {
        ClusteringComparator comparator = oldPartition.metadata().comparator;
        Row oldRow = oldPartition.hasNext() ? oldPartition.next() : null;
        Row newRow = newPartition.hasNext() ? newPartition.next() : null;
        while (oldRow != null && newRow != null)
        {
            int cmp = comparator.compare(oldRow.clustering(), newRow.clustering());
            if (cmp < 0)
            {
                differ.onDropped(oldRow);
                oldRow = oldPartition.hasNext() ? oldPartition.next() : null;
            }
            else if (cmp > 0)
            {
                differ.onAdded(newRow);
                newRow = newPartition.hasNext() ? newPartition.next() : null;
            }
            else
            {
                differ.onUpdated(oldRow, newRow);
                oldRow = oldPartition.hasNext() ? oldPartition.next() : null;
                newRow = newPartition.hasNext() ? newPartition.next() : null;
            }
        }

        while (oldRow != null)
        {
            differ.onDropped(oldRow);
            oldRow = oldPartition.hasNext() ? oldPartition.next() : null;
        }
        while (newRow != null)
        {
            differ.onAdded(oldRow);
            newRow = newPartition.hasNext() ? newPartition.next() : null;
        }
    }
}
