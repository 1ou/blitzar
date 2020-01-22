package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.IndexType;

import java.util.Set;

public interface Index {
    /**
     * Return index type
     *
     * @return index type
     */
    IndexType type();

    /**
     * Fields on which based index
     *
     * @return fields
     */
    Set<String> fields();

    /**
     * Transform index to byte array
     *
     * @return byte array of index data
     */
    byte[] toBytes();
}
