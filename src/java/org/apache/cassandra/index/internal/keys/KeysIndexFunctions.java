package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.ColumnIndexFunctions;
import org.apache.cassandra.index.internal.ColumnIndexMetadata;
import org.apache.cassandra.index.internal.IndexEntry;

public class KeysIndexFunctions implements ColumnIndexFunctions
{
    public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                        CFMetaData baseMetadata,
                                                        ColumnDefinition cfDef)
    {
        // no additional clustering columns required
        return builder;
    }

    public CBuilder buildIndexClusteringPrefix(ColumnIndexMetadata metadata,
                                               ByteBuffer partitionKey,
                                               ClusteringPrefix prefix,
                                               CellPath path)
    {
        CBuilder builder = CBuilder.create(metadata.getIndexComparator());
        builder.add(partitionKey);
        return builder;
    }

    public ByteBuffer getIndexedValue(ColumnIndexMetadata metadata,
                                      ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return cellValue;
    }

    public IndexEntry decodeEntry(ColumnIndexMetadata metadata, DecoratedKey indexedValue, Row indexEntry)
    {
        throw new UnsupportedOperationException("KEYS indexes do not use a specialized index entry format");
    }

    public boolean isStale(ColumnIndexMetadata metadata, Row row, ByteBuffer indexValue, int nowInSec)
    {
        if (row == null)
            return true;

        Cell cell = row.getCell(metadata.indexedColumn);

        return (cell == null
             || !cell.isLive(nowInSec)
             || metadata.indexedColumn.type.compare(indexValue, cell.value()) != 0);
    }
}
