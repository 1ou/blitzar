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
     * Record size
     *
     * @return record size
     */
    int recordSize();
}
