package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.io.IOException;
import java.util.List;

public interface TableDataManager {
    /**
     * Add row
     *
     * @param row row
     * @throws IOException disk error
     */
    void addRow(Row row) throws IOException;

    /**
     * Search by key
     *
     * @param key key
     * @return row
     * @throws IOException disk io issue
     */
    List<Row> search(Key key) throws IOException;

    /**
     * Search
     *
     * @return row
     * @throws IOException disk io issue
     */
    List<Row> search() throws IOException;
}
