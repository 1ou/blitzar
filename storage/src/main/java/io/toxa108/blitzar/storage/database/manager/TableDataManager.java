package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.io.IOException;

public interface TableDataManager {
    /**
     * Add row
     * @param row row
     * @throws IOException disk error
     */
    void addRow(Row row) throws IOException;

    Row search(Key key) throws IOException;
}
