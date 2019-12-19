package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Row;

public interface TableDataManager {
    /**
     * Add row
     * @param row row
     */
    void addRow(Row row);
}
