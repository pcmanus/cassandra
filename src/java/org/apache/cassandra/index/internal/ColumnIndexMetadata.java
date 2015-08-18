package org.apache.cassandra.index.internal;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * Wraps up the specifics about a particular local secondary index,
 * so they can be shared between its Indexers & Searchers.
 */
public class ColumnIndexMetadata
{
    public final IndexMetadata indexMetadata;
    public final ColumnFamilyStore baseCfs;
    public final ColumnFamilyStore indexCfs;
    public final ColumnDefinition indexedColumn;
    public final ColumnIndexFunctions functions;

    public ColumnIndexMetadata(IndexMetadata indexMetadata,
                               ColumnFamilyStore baseCfs,
                               ColumnFamilyStore indexCfs,
                               ColumnDefinition indexedColumn,
                               ColumnIndexFunctions functions)
    {
        this.indexMetadata = indexMetadata;
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

    // todo: should change this to return indexMetadata.name, but there are some places
    // where the index cfs name (i.e. of the form basetable.indexname) is expected, such as
    // input to nodetool rebuild_index and in system.IndexInfo
    public String getIndexName()
    {
        return indexCfs.name;
    }
}
