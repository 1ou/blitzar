package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.util.List;

public interface RowManager {
    /**
     * Add new row
     *
     * @param row row
     */
    void add(Row row);

    /**
     * Search by key
     *
     * @param key key
     * @return rows
     */
    List<Row> search(Key key);

    /**
     * Search
     *
     * @return rows
     */
    List<Row> search();
}
