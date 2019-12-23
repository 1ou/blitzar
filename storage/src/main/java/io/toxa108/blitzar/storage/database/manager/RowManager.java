package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;

public interface RowManager {
    /**
     * Add new row
     * @param row row
     */
    void add(Row row);

    /**
     * Search key
     * @param key key
     * @return row
     */
    Row search(Key key);
}
