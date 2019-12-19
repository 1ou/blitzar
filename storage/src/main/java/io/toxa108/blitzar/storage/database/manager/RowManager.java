package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Row;

public interface RowManager {
    /**
     * Add new row
     * @param row row
     */
    void add(final Row row);
}
