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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.ComponentGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final ComponentGroup.Writer group;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter offsetWriter;
    private final MetadataWriter metadataWriter;

    private long currentKeyPartitionOffset;

    public SSTableComponentsWriter(ComponentGroup.Writer group) throws IOException
    {
        this.group = group;
        this.metadataWriter = new MetadataWriter(group);
        this.tokenWriter = new NumericValuesWriter(group.addOrGet(IndexComponent.TOKEN_VALUES),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(group.addOrGet(IndexComponent.OFFSETS_VALUES),
                                                    metadataWriter, true);
    }

    @Override
    public void startPartition(long position)
    {
        currentKeyPartitionOffset = position;
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        recordCurrentTokenOffset(primaryKey.token().getLongValue(), currentKeyPartitionOffset);
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        group.markComplete();
    }

    @Override
    public void abort(Throwable accumulator)
    {
        logger.debug(group.logMessage("Aborting token/offset writer for {}..."), group.descriptor());
        group.forceDeleteAllComponents();
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }
}
