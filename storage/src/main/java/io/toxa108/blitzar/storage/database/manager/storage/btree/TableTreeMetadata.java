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
     * @throws IOException disk io exception
     */
    int freeSpacePos() throws IOException;

    /**
     * Size of non variable records
     *
     * @return size
     */
    int dataNonVariableRecordSize();

    /**
     * Return primary index non variable size
     *
     * @return primary index non variable size
     */
    int primaryIndexNonVariableRecordSize();

    /**
     * Return max number of entries are able to put in leaf node
     *
     * @return max number of entries are able to put in leaf node
     */
    int entriesInLeafNodeNumber();

    /**
     * Return max number of entries are able to put in non leaf node
     *
     * @return max number of entries are able to put in non leaf node
     */
    int entriesInNonLeafNodeNumber();

    /**
     * Return number of used blocks
     *
     * @return number of used blocks
     * @throws IOException disk io issue
     */
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
