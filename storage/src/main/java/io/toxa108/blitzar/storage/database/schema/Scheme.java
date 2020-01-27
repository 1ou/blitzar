package io.toxa108.blitzar.storage.database.schema;

import java.util.Set;

public interface Scheme {
    /**
     * Retrun list of fields
     *
     * @return fields
     */
    Set<Field> fields();

    /**
     * Return list of indexes
     *
     * @return indexes
     */
    Set<Index> indexes();

    /**
     * Return Primary index
     *
     * @return primary index
     */
    Index primaryIndex();

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
     * Is scheme variable
     *
     * @return variability
     */
    boolean isVariable();

    /**
     * Record size (all fields include primary index)
     *
     * @return record size
     */
    int recordSize();

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

    /**
     * Return field by name
     * @param fieldName field name
     * @return field
     * @throws java.util.NoSuchElementException if field doesn't exist
     */
    Field fieldByName(String fieldName);
}
