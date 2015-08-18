package org.apache.cassandra.index.internal;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.composites.CompositesSearcher;
import org.apache.cassandra.index.internal.keys.KeysSearcher;

public class CassandraIndexSearcher implements Index.Searcher
{
    private final ColumnIndexMetadata metadata;
    private final ReadCommand command;
    private final RowFilter.Expression expression;
    private final CassandraIndex indexer;

    public CassandraIndexSearcher(ColumnIndexMetadata metadata,
                                  ReadCommand command,
                                  RowFilter.Expression expression,
                                  CassandraIndex indexer)
    {
        this.metadata = metadata;
        this.command = command;
        this.expression = expression;
        this.indexer = indexer;
    }

    public RowFilter.Expression getExpression()
    {
        return expression;
    }

    public UnfilteredPartitionIterator search(ReadOrderGroup orderGroup)
    {
        switch (metadata.indexMetadata.indexType)
        {
            case COMPOSITES:
                return new CompositesSearcher(metadata, expression, indexer).search(command, orderGroup);
            case KEYS:
                return new KeysSearcher(metadata, expression, indexer).search(command, orderGroup);
            default:
                throw new IllegalStateException(String.format("Unsupported index type %s for index %s on %s",
                                                              metadata.indexMetadata.indexType,
                                                              metadata.indexMetadata.name,
                                                              metadata.indexedColumn.name.toString()));
        }
    }

}
