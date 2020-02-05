package io.toxa108.blitzar.storage.database.manager.storage.btree;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.schema.Field;

import java.io.IOException;
import java.util.Set;

/**
 * Tooo fucking big
 */
public interface TableTreeMetadata {

    /**
     * Return database configuration
     *
     * @return database configuration
     */
    DatabaseConfiguration databaseConfiguration();

    /**
     * Size of reserved space on each node.
     * 1. 1 byte for leaf or non leaf.
     * 2. 4 byte for number of entries
     *
     * @return size of reserved space
     */

    int reservedSpaceInNode();

    /**
     * Free space position
     *
     * @return position
     */
    int freeSpacePos() throws IOException;

    int dataNonVariableRecordSize();

    int primaryIndexNonVariableRecordSize();

    int estimatedSizeOfElementsInLeafNode();

    int estimatedSizeOfElementsInNonLeafNode();

    int numberOfUsedBlocks() throws IOException;

    /**
     * Return Primary index field
     *
     * @return primary  field
     */
    Field primaryIndexField();

    /**
     * Return size of primary index
     *
     * @return primary index size
     */
    int primaryIndexSize();


    /**
     * Record size (exclude primary index)
     *
     * @return data size
     */
    int dataSize();

    /**
     * Return data fields
     *
     * @return only data fields
     */
    Set<Field> dataFields();

    /**
     * Return true if scheme contains index
     *
     * @param indexName index name
     * @return true or false
     */
    boolean containIndex(String indexName);

}
