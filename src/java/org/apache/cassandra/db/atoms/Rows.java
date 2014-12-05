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
package org.apache.cassandra.db.atoms;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.security.MessageDigest;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private static final Logger logger = LoggerFactory.getLogger(Rows.class);

    private Rows() {}

    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    public static final Row EMPTY_STATIC_ROW = new Row()
    {
        public Columns columns()
        {
            return Columns.NONE;
        }

        public long timestamp()
        {
            return Long.MIN_VALUE;
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean hasComplexDeletion()
        {
            return false;
        }

        public ClusteringPrefix clustering()
        {
            return EmptyClusteringPrefix.STATIC_PREFIX;
        }

        public Cell getCell(ColumnDefinition c)
        {
            return null;
        }

        public Iterator<Cell> getCells(ColumnDefinition c)
        {
            return null;
        }

        public DeletionTime getDeletion(ColumnDefinition c)
        {
            return DeletionTime.LIVE;
        }

        public Iterator<Cell> iterator()
        {
            return Iterators.<Cell>emptyIterator();
        }

        public Kind kind()
        {
            return Atom.Kind.ROW;
        }

        public Row takeAlias()
        {
            return this;
        }
    };

    public interface SimpleMergeListener
    {
        public void onAdded(Cell newCell);
        public void onRemoved(Cell removedCell);
        public void onUpdated(Cell existingCell, Cell updatedCell);
    }

    public static void copy(Row row, Row.Writer writer)
    {
        writer.writeClustering(row.clustering());
        writer.writeTimestamp(row.timestamp());

        for (Cell cell : row)
            Cells.write(cell, writer);

        for (int i = 0; i < row.columns().complexColumnCount(); i++)
        {
            ColumnDefinition c = row.columns().getComplex(i);
            DeletionTime dt = row.getDeletion(c);
            if (!dt.isLive())
                writer.writeComplexDeletion(c, dt);
        }
        writer.endOfRow();
    }

    public static boolean hasLiveData(Row row, int nowInSec)
    {
        if (row == null)
            return false;

        if (row.timestamp() != NO_TIMESTAMP)
            return true;

        for (Cell cell : row)
            if (Cells.isLive(cell, nowInSec))
                return true;

        return false;
    }

    public static Cell getCell(Row row, ColumnDefinition c)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Iterator<Cell> getCells(Row row, ColumnDefinition c)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Cell getCell(Row row, ColumnDefinition c, CellPath path)
    {
        // TODO (for collections, we should do a binary search of the path)
        throw new UnsupportedOperationException();
    }

    public static void digest(Row row, MessageDigest digest)
    {
        FBUtilities.updateWithByte(digest, row.kind().ordinal());
        row.clustering().digest(digest);
        FBUtilities.updateWithLong(digest, row.timestamp());
        Iterator<ColumnDefinition> iter = row.columns().complexColumns();
        while (iter.hasNext())
            row.getDeletion(iter.next()).digest(digest);

        for (Cell cell : row)
            Cells.digest(cell, digest);
    }

    public static String toString(CFMetaData metadata, Row row)
    {
        return toString(metadata, row, false);
    }

    public static String toString(CFMetaData metadata, Row row, boolean fullCellDetails)
    {
        StringBuilder sb = new StringBuilder();
        ClusteringPrefix clustering = row.clustering();
        sb.append("Row");
        if (row.timestamp() != NO_TIMESTAMP)
            sb.append("@").append(row.timestamp());
        sb.append(": ").append(clustering.toString(metadata)).append(" | ");
        boolean isFirst = true;
        for (Cell cell : row)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            if (fullCellDetails)
                sb.append(Cells.toString(cell));
            else
                sb.append(cell.column().name).append("=").append(cell.column().type.getString(cell.value()));
        }
        return sb.toString();
    }

    // Merge rows in memtable
    public static void merge(Row existing,
                             Row update,
                             Columns mergedColumns,
                             Row.Writer writer,
                             int nowInSec,
                             SecondaryIndexManager.Updater indexUpdater)
    {
        ClusteringPrefix clustering = existing.clustering();
        writer.writeClustering(clustering);
        writer.writeTimestamp(Math.max(existing.timestamp(), existing.timestamp()));

        for (int i = 0; i < mergedColumns.simpleColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getSimple(i);
            Cells.reconcile(clustering, existing.getCell(c), update.getCell(c), writer, nowInSec, indexUpdater);
        }

        for (int i = 0; i < mergedColumns.complexColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getComplex(i);
            DeletionTime existingDt = existing.getDeletion(c);
            DeletionTime updateDt = update.getDeletion(c);
            if (existingDt.supersedes(updateDt))
                writer.writeComplexDeletion(c, existingDt);
            else
                writer.writeComplexDeletion(c, updateDt);

            Iterator<Cell> existingCells = existing.getCells(c);
            Iterator<Cell> updateCells = update.getCells(c);
            Cells.reconcileComplex(clustering, c, existingCells, updateCells, writer, nowInSec, indexUpdater);
        }

        writer.endOfRow();
    }

    public static class Serializer
    {
        private final LegacyLayout layout;

        public Serializer(LegacyLayout layout)
        {
            this.layout = layout;
        }

        public void serialize(Row row, DataOutputPlus out)
        {
            throw new UnsupportedOperationException();
            //layout.clusteringSerializer().serialize(row.clustering(), out);
            //out.writeLong(row.timestamp());
            //for (ColumnData data : row)
            //{
            //    ByteBufferUtil.writeWithShortLength(data.column().name.bytes, out);
            //    if (data.column().isComplex())
            //    {
            //        DeletionTime.serializer.serialize(data.complexDeletionTime(), out);
            //        out.writeInt(data.size());
            //    }
            //    for (int i = 0; i < data.size(); i++)
            //        layout.serializeCellBody(data.cell(i), out);
            //}
            //out.writeShort(0);
        }

        public void deserialize(DataInput in, LegacyLayout.Flag flag, Row.Writer writer, CFMetaData metadata)
        {
            throw new UnsupportedOperationException();
            //writer.setClustering(layout.clusteringSerializer().deserialize(in));
            //writer.setTimestamp(in.readLong());
            //int size = in.readUnsignedShort();

            //// TODO: could reuse at a more high level
            //Rows.DeserializedCell cell = new Rows.DeserializedCell();
            //while (size > 0)
            //{
            //    ByteBuffer name = ByteBufferUtil.read(in, size);
            //    ColumnDefinition def = metadata.getColumnDefinition(name);

            //    assert def != null; // TODO - this is possibly fragile
            //    int count;
            //    if (def.isComplex())
            //    {
            //        count = in.readInt();
            //        writer.newColumn(def, DeletionTime.serializer.deserialize(in));
            //    }
            //    else
            //    {
            //        count = 1;
            //        writer.newColumn(def, DeletionTime.LIVE);
            //    }
            //    for (int i = 0; i < count; i++)
            //    {
            //        layout.deserializeCellBody(in, cell);
            //        writer.newCell(cell);
            //    }
            //}
            //writer.endOfRow();
        }

        public long serializedSize(Row row, TypeSizes sizes)
        {
            throw new UnsupportedOperationException();
            //long size = layout.clusteringSerializer().serializedSize(row.clustering(), sizes)
            //          + sizes.sizeof(row.timestamp());

            //for (ColumnData data : row)
            //{
            //    size += ByteBufferUtil.serializedSizeWithShortLength(data.column().name.bytes, sizes);
            //    if (data.column().isComplex())
            //    {
            //        size += DeletionTime.serializer.serializedSize(data.complexDeletionTime(), sizes);
            //        size += sizes.sizeof(data.size());
            //    }
            //    for (int i = 0; i < data.size(); i++)
            //        size += serializedSizeCell(data.cell(i));
            //}
            //size += sizes.sizeof((short)0);
            //return size;
        }
    }

    public static class Merger
    {
        private final int nowInSec;
        private final AtomIterators.MergeListener listener;
        private final Columns columns;

        private ClusteringPrefix clustering;
        private final Row[] rows;

        private final ReusableRow row;
        private final Cell[] cells;
        private final List<Iterator<Cell>> complexCells;
        private final ComplexColumnReducer complexReducer = new ComplexColumnReducer();

        // For the sake of the listener if there is one
        private final DeletionTime[] complexDelTimes;

        public Merger(int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
        {
            this.nowInSec = nowInSec;
            this.listener = listener;
            this.columns = columns;
            this.rows = new Row[size];
            this.row = new ReusableRow(columns);
            this.complexCells = new ArrayList<>(size);

            this.cells = new Cell[size];
            this.complexDelTimes = listener == null ? null : new DeletionTime[size];
        }

        public void clear()
        {
            Arrays.fill(rows, null);
            complexCells.clear();
        }

        public void add(int i, Row row)
        {
            clustering = row.clustering();
            rows[i] = row;
        }

        public Row merge(DeletionTime activeDeletion)
        {
            Row.Writer writer = row.writer();
            writer.writeClustering(clustering);

            long timestamp = Cells.NO_TIMESTAMP;
            for (int i = 0; i < rows.length; i++)
                if (rows[i] != null)
                    timestamp = Math.max(timestamp, rows[i].timestamp());

            timestamp = activeDeletion.deletes(timestamp) ? Cells.NO_TIMESTAMP : timestamp;
            writer.writeTimestamp(timestamp);

            if (listener != null)
                listener.onMergingRows(clustering, timestamp, rows);

            for (int i = 0; i < columns.simpleColumnCount(); i++)
            {
                ColumnDefinition c = columns.getSimple(i);
                for (int j = 0; j < rows.length; j++)
                    cells[j] = rows[j] == null ? null : rows[j].getCell(c);

                reconcileCells(activeDeletion, c, writer);
            }

            complexReducer.activeDeletion = activeDeletion;
            complexReducer.writer = writer;
            for (int i = 0; i < columns.complexColumnCount(); i++)
            {
                ColumnDefinition c = columns.getComplex(i);

                DeletionTime maxComplexDeletion = DeletionTime.LIVE;
                for (int j = 0; j < rows.length; j++)
                {
                    if (rows[j] == null)
                        continue;

                    DeletionTime dt = rows[j].getDeletion(c);
                    if (complexDelTimes != null)
                        complexDelTimes[j] = dt;

                    if (dt.supersedes(maxComplexDeletion))
                        maxComplexDeletion = dt;
                }

                boolean overrideActive = maxComplexDeletion.supersedes(activeDeletion);
                maxComplexDeletion =  overrideActive ? maxComplexDeletion : DeletionTime.LIVE;
                writer.writeComplexDeletion(c, maxComplexDeletion);
                if (listener != null)
                    listener.onMergedComplexDeletion(c, maxComplexDeletion, complexDelTimes);

                mergeComplex(overrideActive ? maxComplexDeletion : activeDeletion, c);
            }
            writer.endOfRow();
            if (listener != null)
                listener.onRowDone();

            // Because shadowed cells are skipped, the row could be empty. In which case
            // we return null.
            return row.isEmpty() ? null : row;
        }

        private void reconcileCells(DeletionTime activeDeletion, ColumnDefinition c, Row.Writer writer)
        {
            Cell reconciled = null;
            for (int j = 0; j < cells.length; j++)
            {
                Cell cell = cells[j];
                if (cell != null && !activeDeletion.deletes(cell.timestamp()))
                    reconciled = Cells.reconcile(reconciled, cell, nowInSec);
            }

            if (reconciled != null)
            {
                Cells.write(reconciled, writer);
                if (listener != null)
                    listener.onMergedCells(reconciled, cells);
            }
        }

        private void mergeComplex(DeletionTime activeDeletion, ColumnDefinition c)
        {
            complexCells.clear();
            for (int j = 0; j < rows.length; j++)
            {
                Row row = rows[j];
                Iterator<Cell> iter = row == null ? null : row.getCells(c);
                complexCells.add(iter == null ? Iterators.<Cell>emptyIterator() : iter);
            }

            complexReducer.column = c;

            // Note that we use the mergeIterator only to group cells to merge, but we
            // write the result to the writer directly in the reducer, so all we care
            // about is iterating over the result.
            Iterator<Void> iter = MergeIterator.get(complexCells, c.cellComparator(), complexReducer);
            while (iter.hasNext())
                iter.next();
        }

        private class ComplexColumnReducer extends MergeIterator.Reducer<Cell, Void>
        {
            private DeletionTime activeDeletion;
            private Row.Writer writer;
            private ColumnDefinition column;

            public void reduce(int idx, Cell current)
            {
                cells[idx] = current;
            }

            protected Void getReduced()
            {
                reconcileCells(activeDeletion, column, writer);
                return null;
            }

            protected void onKeyChange()
            {
                Arrays.fill(cells, null);
            }
        }
    }
}
