package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.IndexType;

import java.util.List;

public interface Index {
    /**
     * Return index type
     * @return index type
     */
    IndexType type();

    /**
     * Fields on which based index
     * @return fields
     */
    List<String> fields();
}
