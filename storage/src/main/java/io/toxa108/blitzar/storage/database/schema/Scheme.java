package io.toxa108.blitzar.storage.database.schema;

import java.util.Set;

public interface Scheme {
    /**
     * Retrun list of fields
     * @return fields
     */
    Set<Field> fields();

    /**
     * Return list of indexes
     * @return indexes
     */
    Set<Index> indexes();
}
