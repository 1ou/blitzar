package io.toxa108.blitzar.storage.database.schema;

import java.util.List;

public interface Scheme {
    /**
     * Retrun list of fields
     * @return fields
     */
    List<Field> fields();

    /**
     * Return list of indexes
     * @return indexes
     */
    List<Index> indexes();
}
