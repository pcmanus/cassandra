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
import java.nio.ByteOrder;

import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.ChecksumIndexInput;

public interface IndexComponentInfo
{
    ComponentGroup group();

    IndexComponent component();

    ByteOrder byteOrder();

    String fileNamePart();
    Component asCustomComponent();
    File file();

    default boolean isCompletionMarker()
    {
        return component() == group().completionMarkerComponent();
    }

    interface Reader extends IndexComponentInfo
    {
        @Override
        ComponentGroup.Reader group();

        FileHandle createFileHandle();

        /**
         * Opens a file handle for the provided index component similarly to {@link #createFileHandle()},
         * but this method shoud be called instead of the aforemented one if the access is done "as part of flushing", that is
         * before the full index that this is a part of has been finalized.
         * <p>
         * The use of this method can allow specific storage providers, typically tiered storage ones, to distinguish accesses
         * that happen "at flush time" from other accesses, as the related file may be in different tier of storage.
         */
        FileHandle createFlushTimeFileHandle();

        IndexInput openInput();

        ChecksumIndexInput openCheckSummedInput();
    }

    interface Writer extends IndexComponentInfo
    {
        @Override
        ComponentGroup.Writer group();

        default IndexOutputWriter openOutput() throws IOException
        {
            return openOutput(false);
        }

        IndexOutputWriter openOutput(boolean append) throws IOException;

        void createEmpty() throws IOException;
    }
}
