package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.FieldType;
import io.toxa108.blitzar.storage.database.schema.impl.Nullable;
import io.toxa108.blitzar.storage.database.schema.impl.Unique;

public interface Field {
    /**
     * Return name
     *
     * @return name
     */
    String name();

    /**
     * Return field type
     *
     * @return field type
     */
    FieldType type();

    /**
     * Return value
     *
     * @return value
     */
    byte[] value();

    /**
     * Return nullable or not
     *
     * @return nullable state
     */
    Nullable nullable();

    /**
     * Return unique or not
     *
     * @return unique state
     */
    Unique unique();

    /**
     * Size to store on disk
     *
     * @return size to store on disk
     */
    int diskSize();

    /**
     * Does size of record change duu to entry
     * @return variability
     */
    boolean isVariable();
}
