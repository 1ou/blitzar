package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.State;

public interface Table {
    /**
     * Name
     *
     * @return name
     */
    String name();

    /**
     * Return state
     *
     * @return state
     */
    State state();

    /**
     * Return scheme
     *
     * @return scheme
     */
    Scheme scheme();

    /**
     * Add row
     * @param row row
     */
    void addRow(Row row);
}
