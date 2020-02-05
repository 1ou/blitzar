package io.toxa108.blitzar.storage.database.manager.row;

import io.toxa108.blitzar.storage.database.schema.Field;
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
     * Search by field
     *
     * @param field field
     * @return rows
     */
    List<Row> search(Field field);

    /**
     * Search
     *
     * @return rows
     */
    List<Row> search();
}
