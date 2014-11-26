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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
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

        // TODO
        throw new UnsupportedOperationException();
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

    public static String toString(CFMetaData metadata, Row row)
    {
        StringBuilder sb = new StringBuilder();
        ClusteringPrefix clustering = row.clustering();
        sb.append("Row");
        if (row.timestamp() != NO_TIMESTAMP)
            sb.append("@").append(row.timestamp());
        sb.append(": ");
        for (int i = 0; i < clustering.size(); i++)
        {
            if (i > 0)
                sb.append(", ");
            ColumnDefinition c = metadata.clusteringColumns().get(i);
            sb.append(c.name).append("=").append(c.type.getString(clustering.get(i)));
        }
        sb.append(" | ");
        boolean isFirst = true;
        for (Cell cell : row)
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            ColumnDefinition c = cell.column();
            sb.append(c.name).append("=").append(c.type.getString(cell.value()));
        }
        return sb.toString();
    }

    // Merge multiple rows that are assumed to represent the same row (same clustering prefix).
    //public static void merge(ClusteringPrefix clustering, Row[] rows, MergeHelper helper, AtomIterators.MergeListener listener)
    //{
    //    throw new UnsupportedOperationException();
    //    //helper.setRows(rows);
    //    //listener.onMergingRows(clustering, helper.maxRowTimestamp, rows);

    //    //while (helper.hasMoreColumns())
    //    //{
    //    //    ColumnDefinition c = helper.columnToMerge;
    //    //    listener.onMergedColumns(c, helper.mergedComplexDeletion(), helper.complexDeletions);

    //    //    while (helper.hasMoreCellsForColumn())
    //    //    {
    //    //        Cell[] versions = helper.cellsToMerge;
    //    //        Cell merged = null;
    //    //        for (int i = 0; i < versions.length; i++)
    //    //        {
    //    //            Cell cell = versions[i];
    //    //            if (cell == null)
    //    //                continue;

    //    //            merged = merged == null ? cell : Cells.reconcile(merged, cell, helper.nowInSec);
    //    //        }
    //    //        listener.onMergedCells(merged, versions);
    //    //    }
    //    //}
    //    //listener.onRowDone();
    //}

    public static void merge(ClusteringPrefix clustering, Row[] rows, int nowInSec, final Row.Writer writer)
    {
        throw new UnsupportedOperationException();
        //merge(clustering, rows, new MergeHelper(nowInSec, rows.length), new AtomIterators.MergeListener()
        //{
        //    public void onMergingRows(ClusteringPrefix clustering, long mergedTimestamp, Row[] versions)
        //    {
        //        writer.setClustering(clustering);
        //        writer.setTimestamp(mergedTimestamp);
        //    }

        //    public void onMergedColumns(ColumnDefinition c, DeletionTime mergedComplexDeletion, DeletionTimeArray versions)
        //    {
        //        writer.newColumn(c, mergedComplexDeletion);
        //    }

        //    public void onMergedCells(Cell mergedCell, Cell[] versions)
        //    {
        //        writer.newCell(mergedCell);
        //    }

        //    public void onRowDone()
        //    {
        //        writer.endOfRow();
        //    }

        //    public void onMergedRangeTombstoneMarkers(ClusteringPrefix prefix, boolean isOpenMarker, DeletionTime mergedDelTime, RangeTombstoneMarker[] versions)
        //    {
        //    }

        //    public void close()
        //    {
        //    }
        //});
    }

    // Merge rows in memtable
    public static void merge(Row existing,
                             Row update,
                             Row.Writer writer,
                             int nowInSec,
                             SecondaryIndexManager.Updater indexUpdater)
    {
        writer.setClustering(existing.clustering());
        writer.setTimestamp(Math.max(existing.timestamp(), existing.timestamp()));



        throw new UnsupportedOperationException();

        //Iterator<ColumnData> it1 = r1.iterator();
        //Iterator<ColumnData> it2 = r2.iterator();

        //ColumnData c1 = it1.hasNext() ? it1.next() : null;
        //ColumnData c2 = it2.hasNext() ? it2.next() : null;

        //while (c1 != null && c2 != null)
        //{
        //    int cmp = c1.column().compareTo(c2.column());
        //    if (cmp < 0)
        //    {
        //        writer.newColumn(c1.column(), c1.complexDeletionTime());
        //        for (int i = 0; i < c1.size(); i++)
        //            writer.newCell(c1.cell(i));
        //        c1 = it1.hasNext() ? it1.next() : null;
        //    }
        //    else if (cmp > 0)
        //    {
        //        writer.newColumn(c2.column(), c2.complexDeletionTime());
        //        for (int i = 0; i < c2.size(); i++)
        //        {
        //            Cell cell = c2.cell(i);
        //            indexUpdater.insert(clustering, c2.column(), cell);
        //            writer.newCell(c2.cell(i));
        //        }
        //        c2 = it2.hasNext() ? it2.next() : null;
        //    }
        //    else
        //    {
        //        DeletionTime dt = c1.complexDeletionTime().supersedes(c2.complexDeletionTime())
        //                        ? c1.complexDeletionTime()
        //                        : c2.complexDeletionTime();
        //        writer.newColumn(c1.column(), dt);
        //        int i1 = 0;
        //        int i2 = 0;
        //        Comparator<CellPath> comparator = c1.column().cellPathComparator();
        //        while (i1 < c1.size() && i2 < c2.size())
        //        {
        //            Cell cell1 = c1.cell(i1);
        //            Cell cell2 = c2.cell(i1);
        //            cmp = comparator.compare(cell1.path(), cell2.path());
        //            if (cmp < 0)
        //            {
        //                writer.newCell(cell1);
        //                i1++;
        //            }
        //            else if (cmp > 0)
        //            {
        //                indexUpdater.insert(clustering, c2.column(), cell1);
        //                writer.newCell(cell2);
        //                i2++;
        //            }
        //            else
        //            {
        //                Cell reconciled = Cells.reconcile(cell1, cell2, nowInSec);
        //                indexUpdater.update(clustering, c1.column(), cell1, reconciled);
        //                writer.newCell(reconciled);
        //                i1++;
        //                i2++;
        //            }
        //        }
        //        for (; i1 < c1.size(); i1++)
        //        {
        //            writer.newCell(c1.cell(i1));
        //            i1++;
        //        }
        //        for (; i2 < c2.size(); i2++)
        //        {
        //            Cell cell = c2.cell(i2);
        //            indexUpdater.insert(clustering, c2.column(), cell);
        //            writer.newCell(cell);
        //            i1++;
        //        }
        //    }
        //}
        //writer.endOfRow();
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

    /**
     * Utility object to merge multiple rows.
     * <p>
     * We don't want to use a MergeIterator to merge multiple rows because we do that often
     * in the process of merging AtomIterators and we don't want to allocate iterators every
     * time (this object is reused over the course of merging multiple AtomIterator) and is
     * overall cheaper by being specialized.
     */
    //static class MergeHelper
    //{
    //    public final int nowInSec;
    //    private final int size;

    //    private Row[] rows;
    //    private long maxRowTimestamp;

    //    private final ColumnData[] columns;
    //    private final Iterator<ColumnData>[] columnIterators;

    //    private ColumnDefinition columnToMerge;
    //    private final ColumnData[] columnsDataToMerge;
    //    private final DeletionTimeArray complexDeletions;
    //    private final DeletionTimeArray.Cursor complexDeletionsCursor;
    //    private int maxComplexDeletion;

    //    private final int[] remainings;
    //    private final Cell[] cellsToMerge;

    //    public MergeHelper(int nowInSec, int size)
    //    {
    //        this.nowInSec = nowInSec;
    //        this.size = size;

    //        this.columns = new ColumnData[size];
    //        this.columnIterators = (Iterator<ColumnData>[]) new Iterator[size];

    //        this.columnsDataToMerge = new ColumnData[size];
    //        this.complexDeletions = new DeletionTimeArray(size);
    //        this.complexDeletionsCursor = complexDeletions.newCursor();
    //        this.remainings = new int[size];
    //        this.cellsToMerge = new Cell[size];
    //    }

    //    public void setRows(Row[] rows)
    //    {
    //        this.rows = rows;
    //        this.maxRowTimestamp = Long.MIN_VALUE;
    //        for (int i = 0; i < rows.length; i++)
    //        {
    //            Row r = rows[i];
    //            if (r == null)
    //            {
    //                columnIterators[i] = Collections.<ColumnData>emptyIterator();
    //            }
    //            else
    //            {
    //                maxRowTimestamp = Math.max(maxRowTimestamp, r.timestamp());
    //                columnIterators[i] = r.iterator();
    //            }
    //        }
    //    }

    //    private void reset(int prevMin, int i)
    //    {
    //        for (int j = prevMin; j < i; j++)
    //        {
    //            if (cellsToMerge[j] != null)
    //            {
    //                cellsToMerge[j] = null;
    //                ++remainings[i];
    //            }
    //        }
    //    }

    //    public boolean hasMoreColumns()
    //    {
    //        columnToMerge = null;
    //        maxComplexDeletion = -1;
    //        for (int i = 0; i < size; i++)
    //        {
    //            columnsDataToMerge[i] = null;
    //            complexDeletions.clear(i);

    //            // Are we done with that iterator
    //            ColumnData d = columns[i];
    //            if (d == null)
    //            {
    //                if (!columnIterators[i].hasNext())
    //                    continue;
    //                d = columnIterators[i].next();
    //                columns[i] = d;
    //            }

    //            ColumnDefinition c = d.column();
    //            if (columnToMerge == null || columnToMerge.compareTo(c) > 0)
    //                columnToMerge = c;
    //        }

    //        if (columnToMerge == null)
    //            return false;

    //        // We found the next column to merge, set remainings for it
    //        for (int i = 0; i < size; i++)
    //        {
    //            ColumnData d = columns[i];
    //            if (d != null && d.column().equals(columnToMerge))
    //            {
    //                columnsDataToMerge[i] = d;
    //                complexDeletions.set(i, d.complexDeletionTime());
    //                if (complexDeletions.isLive(i) && (maxComplexDeletion < 0 || complexDeletions.supersedes(i, maxComplexDeletion)))
    //                    maxComplexDeletion = i;
    //                remainings[i] = d.size();
    //                columns[i] = null;
    //            }
    //        }
    //        return true;
    //    }

    //    private int select(int i, Cell candidate)
    //    {
    //        cellsToMerge[i] = candidate;
    //        --remainings[i];
    //        return i;
    //    }

    //    public boolean hasMoreCellsForColumn()
    //    {
    //        int minCell = -1;
    //        for (int i = 0; i < size; i++)
    //        {
    //            cellsToMerge[i] = null;
    //            int remaining = remainings[i];
    //            if (remaining == 0)
    //                continue;

    //            ColumnData d = columnsDataToMerge[i];
    //            if (!d.column().isComplex())
    //            {
    //                select(i, d.cell(0));
    //                continue;
    //            }

    //            Cell candidate = d.cell(d.size() - remaining);
    //            if (minCell == -1)
    //            {
    //                minCell = select(i, candidate);
    //                continue;
    //            }

    //            Comparator<CellPath> comparator = d.column().cellPathComparator();
    //            int cmp = comparator.compare(candidate.path(), cellsToMerge[minCell].path());
    //            if (cmp < 0)
    //            {
    //                // We've found a smaller cell, 'reset' cellsToMerge and set the candidate
    //                reset(minCell, i);
    //                minCell = select(i, candidate);
    //            }
    //            else if (cmp == 0)
    //            {
    //                select(i, candidate);
    //            }
    //        }

    //        return minCell >= 0;
    //    }

    //    public DeletionTime mergedComplexDeletion()
    //    {
    //        return maxComplexDeletion < 0 ? DeletionTime.LIVE : complexDeletionsCursor.setTo(maxComplexDeletion);
    //    }
    //}
}
