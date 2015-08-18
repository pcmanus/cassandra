package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.index.internal.composites.*;
import org.apache.cassandra.index.internal.keys.KeysIndexFunctions;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * Implementations provide specializations for dealing with entries in the various
 * different types of internal index. Variations are based on the characteristics of
 * indexed column; i.e. its kind, the storage for the table and for collections,
 * whether the collection keys/values/entries are the index target.
 */
public interface ColumnIndexFunctions
{
    /**
     * Used to construct an the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data from the base
     * table and varies depending on the kind of the indexed column. See indexCfsMetadata for more details
     * Used whenever a row in the index table is written or deleted.
     * @param metadata
     * @param partitionKey from the base data being indexed
     * @param prefix from the base data being indexed
     * @param path from the base data being indexed
     * @return a clustering prefix to be used to insert into the index table
     */
    CBuilder buildIndexClusteringPrefix(ColumnIndexMetadata metadata,
                                        ByteBuffer partitionKey,
                                        ClusteringPrefix prefix,
                                        CellPath path);

    /**
     * Used at search time to convert a row in the index table into a simple struct containing the values required
     * to retrieve the corresponding row from the base table.
     * @param metadata
     * @param indexedValue the partition key of the indexed table (i.e. the value that was indexed)
     * @param indexEntry a row from the index table
     * @return
     */
    IndexEntry decodeEntry(ColumnIndexMetadata metadata,
                           DecoratedKey indexedValue,
                           Row indexEntry);

    /**
     * Check whether a value retrieved from an index is still valid by comparing it to current row from the base table.
     * Used at read time to identify out of date index entries so that they can be excluded from search results and
     * repaired
     * @param metadata required to get the indexed column definition
     * @param row the current row from the primary data table
     * @param indexValue the value we retrieved from the index
     * @param nowInSec
     * @return true if the index is out of date and the entry should be dropped
     */
    boolean isStale(ColumnIndexMetadata metadata, Row row, ByteBuffer indexValue, int nowInSec);

    /**
     * Extract the value to be inserted into the index from the components of the base data
     * @param metadata
     * @param partitionKey from the primary data
     * @param clustering from the primary data
     * @param path from the primary data
     * @param cellValue from the primary data
     * @return a ByteBuffer containing the value to be inserted in the index. This will be used to make the partition
     * key in the index table
     */
    ByteBuffer getIndexedValue(ColumnIndexMetadata metadata,
                               ByteBuffer partitionKey,
                               Clustering clustering,
                               CellPath path,
                               ByteBuffer cellValue);

    /**
     * Returns the type of the the values in the index. For most columns this is simply its type, but for collections
     * it depends on whether the index is on the collection name/value element or on a frozen collection
     * @param indexedColumn
     * @return
     */
    default AbstractType<?> getIndexValueType(ColumnDefinition indexedColumn)
    {
        return indexedColumn.type;
    }

    /**
     * Returns true if an index of this type can support search predicates of the form [column] OPERATOR [value]
     * @param indexedColumn
     * @param operator
     * @return
     */
    default boolean supportsOperator(ColumnDefinition indexedColumn, Operator operator)
    {
        return operator.equals(Operator.EQ);
    }

    /**
     * Construct the CFMetadata for an index table, the clustering columns in the index table
     * vary dependent on the kind of the indexed value.
     * @param baseCfsMetadata
     * @param indexMetadata
     * @return
     */
    default CFMetaData indexCfsMetadata(CFMetaData baseCfsMetadata, IndexMetadata indexMetadata)
    {
        ColumnDefinition indexedColumn = indexMetadata.indexedColumn(baseCfsMetadata);
        CFMetaData.Builder builder = CFMetaData.Builder.create(baseCfsMetadata.ksName,
                                                               baseCfsMetadata.indexColumnFamilyName(indexMetadata))
                                                       .withId(baseCfsMetadata.cfId)
                                                       .withPartitioner(new LocalPartitioner(getIndexValueType(indexedColumn)))
                                                       .addPartitionKey(indexedColumn.name, indexedColumn.type);

        builder.addClusteringColumn("partition_key", baseCfsMetadata.partitioner.partitionOrdering());
        builder = addIndexClusteringColumns(builder, baseCfsMetadata, indexedColumn);
        return builder.build().reloadIndexMetadataProperties(baseCfsMetadata);
    }

    /**
     * Add the clustering columns for a specific type of index table to the a CFMetaData.Builder (which is being
     * used to construct the index table's CFMetadata. In the default implementation, the clustering columns of the
     * index table hold the partition key & clustering columns of the base table. This is overridden in several cases:
     * * When the indexed value is itself a clustering column, in which case, we only need store the base table's
     *   *other* clustering values in the index - the indexed value being the index table's partition key
     * * When the indexed value is a collection value, in which case we also need to capture the cell path from the base
     *   table
     * * In a KEYS index (for thrift/compact storage/static column indexes), where only the base partition key is
     *   held in the index table.
     *
     * Called from indexCfsMetadata
     * @param builder
     * @param baseMetadata
     * @param cfDef
     * @return
     */
    default CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                         CFMetaData baseMetadata,
                                                         ColumnDefinition cfDef)
    {
        for (ColumnDefinition def : baseMetadata.clusteringColumns())
            builder.addClusteringColumn(def.name, def.type);
        return builder;
    }

    /**
     * Factory method for obtaining a set of functions based on the type of column the index is on
     * @param cfDef the indexed column
     * @return
     */
    static ColumnIndexFunctions getFunctions(CFMetaData baseCfMetadata, IndexMetadata indexDef)
    {
        if (indexDef.isKeys())
            return new KeysIndexFunctions();

        ColumnDefinition indexedColumn = indexDef.indexedColumn(baseCfMetadata);
        if (indexedColumn.type.isCollection() && indexedColumn.type.isMultiCell())
        {
            switch (((CollectionType)indexedColumn.type).kind)
            {
                case LIST:
                    return new CollectionValueIndexFunctions();
                case SET:
                    return new CollectionKeyIndexFunctions();
                case MAP:
                    if (indexDef.options.containsKey(IndexTarget.INDEX_KEYS_OPTION_NAME))
                        return new CollectionKeyIndexFunctions();
                    else if (indexDef.options.containsKey(IndexTarget.INDEX_ENTRIES_OPTION_NAME))
                        return new CollectionEntryIndexFunctions();
                    else
                        return new CollectionValueIndexFunctions();
            }
        }

        switch (indexedColumn.kind)
        {
            case CLUSTERING:
                return new ClusteringKeyIndexFunctions();
            case REGULAR:
                return new RegularColumnIndexFunctions();
            case PARTITION_KEY:
                return new PartitionKeyIndexFunctions();
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }
}
