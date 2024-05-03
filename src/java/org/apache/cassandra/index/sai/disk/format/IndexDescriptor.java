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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.oldlucene.EndiannessReverserChecksumIndexInput;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The `IndexDescriptor` is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of {@link StorageAttachedIndex}es.
 * <p>
 * The `IndexDescriptor` is primarily responsible for maintaining a view of the on-disk state
 * of the SAI indexes for a specific {@link org.apache.cassandra.io.sstable.SSTable}. It maintains mappings
 * of the current on-disk components and files. It is responsible for opening files for use by
 * writers and readers.
 * <p>
 * Each sstable has per-index components ({@link IndexComponent}) associated with it, and also components
 * that are shared by all indexes (notably, the components that make up the PrimaryKeyMap).
 * <p>
 * IndexDescriptor's remaining responsibility is to act as a proxy to the {@link OnDiskFormat}
 * associated with the index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // TODO Because indexes can be added at any time to existing data, the Version of a column index
    // may not match the Version of the base sstable.  OnDiskFormat + IndexFeatureSet + IndexDescriptor
    // was not designed with this in mind, leading to some awkwardness, notably in IFS where some features
    // are per-sstable (`isRowAware`) and some are per-column (`hasVectorIndexChecksum`).

    // per-SSTable fields
    public final Descriptor descriptor;
    public final IPartitioner partitioner;
    public final ClusteringComparator clusteringComparator;
    public final PrimaryKey.Factory primaryKeyFactory;

    // For each context (null is used for per-sstable ones), the concrete set of existing and "active" components (it is
    // possible for multiple version and/or generation of a component to exists "on disk"; the "active" one is the most
    // recent version and generation that is fully built (has a completion marker)).
    private final Map<IndexContext, ComponentGroupImpl> groups = Maps.newHashMap();

    private IndexDescriptor(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.descriptor = descriptor;
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;

        // Populating the per-sstable components. Not that this needs to happen first to have the proper version below.
        populatePerSSTableComponents(this);
        assert groups.containsKey(null);

        this.primaryKeyFactory = PrimaryKey.factory(clusteringComparator, getVersion().onDiskFormat().indexFeatureSet());
    }

    private static void populatePerSSTableComponents(IndexDescriptor descriptor)
    {
        populateComponents(descriptor, null);
    }

    private static void maybePopulateComponents(IndexDescriptor descriptor, IndexContext context)
    {
        if (!descriptor.groups.containsKey(context))
            populateComponents(descriptor, context);

        assert descriptor.groups.containsKey(context);
    }

    private static void populateComponents(IndexDescriptor descriptor, @Nullable IndexContext context)
    {
        // We first collect all the version/generation for which we have files on disk.
        String indexName = context == null ? null : context.getIndexName();
        Map<Version, IntObjectMap<Set<IndexComponent>>> candidates = Maps.newHashMap();

        PathUtils.forEach(descriptor.descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            // First, we skip any file that do not belong to the sstable this is a descriptor for.
            if (!filename.startsWith(descriptor.descriptor.filenamePart()))
                return;

            // Then we try parsing it as an SAI index file name, and if it matches and is for the requested context,
            // add it to the candidates.
            Version.tryParseFileName(filename)
                   .ifPresent(parsed -> {
                       if (Objects.equals(parsed.indexName, indexName))
                       {
                           candidates.computeIfAbsent(parsed.version, __ -> new IntObjectHashMap<>())
                                     .computeIfAbsent(parsed.generation, __ -> EnumSet.noneOf(IndexComponent.class))
                                     .add(parsed.component);
                       }
                   });
        });

        // The "active" components are then the most recent generation of the most recent version for which we have
        // a completion marker.
        IndexComponent completionMarker = context == null
                                          ? IndexComponent.GROUP_COMPLETION_MARKER
                                          : IndexComponent.COLUMN_COMPLETION_MARKER;

        int maxGenerationForLatest = -1;
        for (Version version : Version.ALL)
        {
            IntObjectMap<Set<IndexComponent>> versionCandidates = candidates.get(version);
            if (versionCandidates == null)
                continue;

            // We track both the max generation this is complete, and the max seen. We'll populate with the max complete
            // one, but we remenber the max seen so that if we have to create a new group, we can start with a generation
            // that does not override any existing files. In general, re-using the generation of an incomplete group
            // would be fine since the file of an incomplete should never have been used anywhere, but in practice, we
            // delete the completion marker on finding corruption as a mean to "invalidate" the component group, and in
            // that case the group file may be in use somewhere. We could add some sort of "corruption" marker component
            // to distinguish between the 2 cases, but this doesn't really seem worth the trouble in practice.
            int maxGeneration = -1;
            int maxCompletedGeneration = -1;

            for (var entry : versionCandidates.entrySet())
            {
                int generation = entry.getKey();
                if (entry.getValue().contains(completionMarker))
                    maxCompletedGeneration = Math.max(maxCompletedGeneration, generation);
                maxGeneration = Math.max(maxGeneration, generation);
            }

            if (version == Version.latest())
                maxGenerationForLatest = maxGeneration;

            if (maxCompletedGeneration >= 0)
            {
                assert maxGeneration >= maxCompletedGeneration;
                ComponentGroupImpl group = descriptor.new ComponentGroupImpl(context, version, maxCompletedGeneration, maxGeneration + 1);
                versionCandidates.get(maxCompletedGeneration).forEach(group::addOrGet);
                group.isComplete = true;
                descriptor.groups.put(context, group);
                return;
            }
        }

        // If we get here, we haven't found any set of valid components. We register an empty group "marker" for the current
        // version (but invalid generation -1) to avoid re-scanning the disk for the same result (and indicate we now know
        // what version/generation we should use for a new build).
        int initialGeneration = maxGenerationForLatest + 1;
        descriptor.groups.put(context, descriptor.new ComponentGroupImpl(context, Version.latest(), -1, initialGeneration));
    }

    public static IndexDescriptor create(SSTableReader sstable)
    {
        return create(sstable.descriptor, sstable.metadata());
    }

    public static IndexDescriptor create(Descriptor descriptor, TableMetadata metadata)
    {
        return create(descriptor, metadata.partitioner, metadata.comparator);
    }

    // Should not be used directly. Only exists for tests.
    @VisibleForTesting
    public static IndexDescriptor create(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(descriptor, partitioner, clusteringComparator);
    }

    public ComponentGroup.Reader perSSTableGroup()
    {
        return groups.get(null);
    }

    public ComponentGroup.Reader perIndexGroup(IndexContext context)
    {
        maybePopulateComponents(this, context);
        return groups.get(context);
    }

    public ComponentGroup.Writer newPerSSTableGroupWriter()
    {
        return newGroupWriter(null);
    }

    public ComponentGroup.Writer newPerIndexGroupWriter(IndexContext context)
    {
        maybePopulateComponents(this, context);
        return newGroupWriter(context);
    }

    private ComponentGroup.Writer newGroupWriter(@Nullable IndexContext context)
    {
        var currentGroup = groups.get(context);
        // If we're "bumping" the version compared to the existing group, then we can default the generation to 0;
        // Otherwise, we trust what the current group says should be the next generation.
        // Unless we don't use immutable components, in which case we always use generation 0.
        Version newVersion = Version.latest();
        int generation = currentGroup.version().equals(newVersion) && newVersion.useImmutableComponentFiles()
                         ? currentGroup.nextGeneration
                         : 0;
        return new ComponentGroupImpl(context, newVersion, generation, generation + 1);
    }

    public Version getVersion()
    {
        return perSSTableGroup().version();
    }

    public Version getVersion(IndexContext context)
    {
        return perIndexGroup(context).version();
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable) throws IOException
    {
        return getVersion().onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext context)
    {
        return isIndexEmpty(context)
               ? new EmptyIndex()
               : getVersion(context).onDiskFormat().newSearchableIndex(sstableContext, context);
    }

    public PerSSTableWriter newPerSSTableWriter() throws IOException
    {
        return perSSTableGroup().version().onDiskFormat().newPerSSTableWriter(this);
    }

    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            long keyCount)
    {
        return Version.latest().onDiskFormat().newPerIndexWriter(index, this, tracker, rowMapping, keyCount);
    }

    /**
     * Returns true if the per-column index components have been built and are valid.
     *
     * @param context The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexContext context)
    {
        return perSSTableGroup().isComplete() && perIndexGroup(context).isComplete();
    }

    public boolean isSSTableEmpty()
    {
        return perSSTableGroup().isEmpty();
    }

    public boolean isIndexEmpty(IndexContext context)
    {
        return perSSTableGroup().isComplete() && perIndexGroup(context).isEmpty();
    }

    public boolean validatePerIndexComponents(IndexContext context)
    {
        return perIndexGroup(context).validateComponents(false);
    }

    public boolean validatePerIndexComponentsChecksum(IndexContext context)
    {
        return perIndexGroup(context).validateComponents(true);
    }

    public boolean validatePerSSTableComponents()
    {
        return perSSTableGroup().validateComponents(false);
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        return perSSTableGroup().validateComponents(true);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor, getVersion());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equals(descriptor, other.descriptor) &&
               Objects.equals(getVersion(), other.getVersion());
    }

    @Override
    public String toString()
    {
        return descriptor.toString() + "-SAI";
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             message);
    }

    private static void deleteComponentFile(File file)
    {
        logger.debug("Deleting storage attached index component file {}", file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn("Unable to delete storage attached index component file {} due to {}.", file, e.getMessage(), e);
        }
    }

    private class ComponentGroupImpl implements ComponentGroup.Writer
    {
        private final @Nullable IndexContext context;
        private final Version version;
        private final int generation;
        private final int nextGeneration;

        private final Map<IndexComponent, IndexComponentInfoImpl> components = new EnumMap<>(IndexComponent.class);

        // Mark groups that are complete (and should not have new components added).
        private volatile boolean isComplete;

        private ComponentGroupImpl(@Nullable IndexContext context, Version version, int generation, int nextGeneration)
        {
            this.context = context;
            this.version = version;
            this.generation = generation;
            this.nextGeneration = nextGeneration;
        }

        @Override
        public Descriptor descriptor()
        {
            return descriptor;
        }

        @Override
        public IndexDescriptor indexDescriptor()
        {
            return IndexDescriptor.this;
        }

        @Nullable
        @Override
        public IndexContext context()
        {
            return context;
        }

        @Override
        public Version version()
        {
            return version;
        }

        @Override
        public int generation()
        {
            return generation;
        }

        @Override
        public boolean has(IndexComponent component)
        {
            return components.containsKey(component);
        }

        @Override
        public boolean isEmpty()
        {
            return isComplete() && components.size() == 1;
        }

        @Override
        public Collection<IndexComponentInfo.Reader> allComponents()
        {
            return Collections.unmodifiableCollection(components.values());
        }

        @Override
        public boolean validateComponents(boolean validateChecksum)
        {
            if (isEmpty())
                return true;

            boolean isValid = true;
            for (IndexComponent expected : expectedComponentsForVersion())
            {
                var component = components.get(expected);
                if (component == null)
                {
                    logger.warn(logMessage("Missing index component {} from SSTable {}"), expected, descriptor);
                    isValid = false;
                }
                else if (!version().onDiskFormat().validateIndexComponent(component, validateChecksum))
                {
                    logger.warn(logMessage("Invalid/corrupted component {} for SSTable {}"), expected, descriptor);
                    if (CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                    {
                        // We delete the corrupted file. Yes, this may break ongoing reads to that component, but
                        // if something is wrong with the file, we're rather fail loudly from that point on than
                        // risking reading and returning corrupted data.
                        deleteComponentFile(component.file());
                        // Note that invalidation will also delete the completion marker
                    }
                    else
                    {
                        logger.debug("Leaving believed-corrupt component {} of SSTable {} in place because {} is false", expected, descriptor, CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getKey());
                    }

                    isValid = false;
                }
            }
            if (!isValid)
                invalidate();
            return isValid;
        }

        @Override
        public void invalidate()
        {
            // We delete the completion marker, to make it clear that group of components shouldn't be used anymore,
            // in particular for the following "populate" call. Note it's comparatively safe to do so in that the
            // marker is never accessed during reads, so we cannot break ongoing operations here.
            var marker = components.remove(completionMarkerComponent());
            if (marker != null)
                deleteComponentFile(marker.file());

            // Keeping legacy behavior if immutable components is disabled.
            if (!version.useImmutableComponentFiles() && CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                forceDeleteAllComponents();

            groups.remove(context);
            populateComponents(IndexDescriptor.this, context);
        }

        @Override
        public Writer asWriter()
        {
            // The difference between Reader and Writer is just to make code cleaner and make it clear when we read
            // components from when we write/modify them. But this concrete implementatation is both in practice.
            return this;
        }

        @Override
        public IndexComponentInfo.Reader get(IndexComponent component)
        {
            IndexComponentInfoImpl info = components.get(component);
            Preconditions.checkNotNull(info, "SSTable %s has no %s component for version %s and generation %s", descriptor, component, version, generation);
            return info;
        }

        @Override
        public long liveSizeOnDiskInBytes()
        {
            return components.values().stream().map(IndexComponentInfoImpl::file).mapToLong(File::length).sum();
        }

        @Override
        public IndexComponentInfo.Writer addOrGet(IndexComponent component)
        {
            Preconditions.checkArgument(!isComplete, "Should not add components to index group for SSTable %s at this point; the completion marker has already been written", descriptor);
            // When a sstable doesn't have any complete group, we use a marker empty one with a generation of -1:
            Preconditions.checkArgument(generation >= 0, "Should not be adding component to empty marker group");
            return components.computeIfAbsent(component, IndexComponentInfoImpl::new);
        }

        @Override
        public void forceDeleteAllComponents()
        {
            components.values()
                      .stream()
                      .map(IndexComponentInfoImpl::file)
                      .forEach(IndexDescriptor::deleteComponentFile);
            components.clear();
        }

        @Override
        public void markComplete() throws IOException
        {
            addOrGet(completionMarkerComponent()).createEmpty();
            isComplete = true;
            groups.put(context, this);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(descriptor, context, version, generation);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComponentGroupImpl that = (ComponentGroupImpl) o;
            return Objects.equals(descriptor, that.descriptor())
                   && Objects.equals(context, that.context)
                   && Objects.equals(version, that.version)
                   && generation == that.generation;
        }

        @Override
        public String toString()
        {
            return String.format("%s components for %s (v: %s, gen: %d): %s",
                                 context == null ? "Per-SSTable" : "Per-Index",
                                 descriptor,
                                 version,
                                 generation,
                                 components.values());
        }

        private class IndexComponentInfoImpl implements IndexComponentInfo.Reader, IndexComponentInfo.Writer
        {
            private final IndexComponent component;

            private volatile String filenamePart;
            private volatile File file;

            private IndexComponentInfoImpl(IndexComponent component)
            {
                this.component = component;
            }

            @Override
            public ComponentGroupImpl group()
            {
                return ComponentGroupImpl.this;
            }

            @Override
            public IndexComponent component()
            {
                return component;
            }

            @Override
            public ByteOrder byteOrder()
            {
                return version.onDiskFormat().byteOrderFor(component, context);
            }

            @Override
            public String fileNamePart()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (filenamePart == null)
                    filenamePart = version.fileNameFormatter().format(component, context, generation);
                return filenamePart;
            }

            @Override
            public Component asCustomComponent()
            {
                return new Component(Component.Type.CUSTOM, fileNamePart());
            }

            @Override
            public File file()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (file == null)
                    file = descriptor.fileFor(asCustomComponent());
                return file;
            }

            @Override
            public FileHandle createFileHandle()
            {
                try (final FileHandle.Builder builder = StorageProvider.instance.fileHandleBuilderFor(this))
                {
                    return builder.order(byteOrder()).complete();
                }
            }

            @Override
            public FileHandle createFlushTimeFileHandle()
            {
                try (final FileHandle.Builder builder = StorageProvider.instance.flushTimeFileHandleBuilderFor(this))
                {
                    return builder.order(byteOrder()).complete();
                }
            }

            @Override
            public IndexInput openInput()
            {
                return IndexFileUtils.instance.openBlockingInput(createFileHandle());
            }

            @Override
            public ChecksumIndexInput openCheckSummedInput()
            {
                var indexInput = openInput();
                return checksumIndexInput(indexInput);
            }

            /**
             * Returns a ChecksumIndexInput that reads the indexInput in the correct endianness for the context.
             * These files were written by the Lucene {@link org.apache.lucene.store.DataOutput}. When written by
             * Lucene 7.5, {@link org.apache.lucene.store.DataOutput} wrote the file using big endian formatting.
             * After the upgrade to Lucene 9, the {@link org.apache.lucene.store.DataOutput} writes in little endian
             * formatting.
             *
             * @param indexInput The index input to read
             * @return A ChecksumIndexInput that reads the indexInput in the correct endianness for the context
             */
            private ChecksumIndexInput checksumIndexInput(IndexInput indexInput)
            {
                if (version == Version.AA)
                    return new EndiannessReverserChecksumIndexInput(indexInput);
                else
                    return new BufferedChecksumIndexInput(indexInput);
            }

            @Override
            public IndexOutputWriter openOutput(boolean append) throws IOException
            {
                File file = file();

                if (logger.isTraceEnabled())
                    logger.trace(group().logMessage("Creating SSTable attached index output for component {} on file {}..."),
                                 component,
                                 file);

                return IndexFileUtils.instance.openOutput(file, byteOrder(), append);
            }

            @Override
            public void createEmpty() throws IOException
            {
                com.google.common.io.Files.touch(file().toJavaIOFile());
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(group(), component);
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                IndexComponentInfoImpl that = (IndexComponentInfoImpl) o;
                return Objects.equals(group(), that.group())
                       && component == that.component;
            }

            @Override
            public String toString()
            {
                return file().toString();
            }
        }
    }
}
