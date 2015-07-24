package org.apache.cassandra.index.internal;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Wraps up the specifics about a particular local secondary index,
 * so they can be shared between its Indexers & Searchers.
 */
public class ColumnIndexMetadata
{
    public final ColumnFamilyStore baseCfs;
    public final ColumnFamilyStore indexCfs;
    public final ColumnDefinition indexedColumn;
    public final ColumnIndexFunctions functions;

    public ColumnIndexMetadata(ColumnFamilyStore baseCfs,
                               ColumnFamilyStore indexCfs,
                               ColumnDefinition indexedColumn,
                               ColumnIndexFunctions functions)
    {
        this.baseCfs = baseCfs;
        this.indexCfs = indexCfs;
        this.indexedColumn = indexedColumn;
        this.functions = functions;
    }

    public long estimateResultRows()
    {
        return indexCfs.getMeanColumns();
    }

    public ClusteringComparator getIndexComparator()
    {
        return indexCfs.metadata.comparator;
    }

    public String getIndexName()
    {
        return indexCfs.name;
    }
}
