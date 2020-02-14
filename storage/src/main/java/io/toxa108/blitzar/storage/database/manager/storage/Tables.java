package io.toxa108.blitzar.storage.database.manager.storage;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.io.IOException;
import java.util.List;

public interface Tables {
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
     * @param field field
     * @return row
     * @throws IOException disk io issue
     */
    List<Row> search(Field field) throws IOException;

    /**
     * Search
     *
     * @return row
     * @throws IOException disk io issue
     */
    List<Row> search() throws IOException;
}
