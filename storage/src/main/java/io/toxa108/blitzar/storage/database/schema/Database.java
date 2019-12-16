package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.State;

import java.util.Optional;

public interface Database {
    /**
     * Return database name
     *
     * @return database name
     */
    String name();

    /**
     * Find table by name
     *
     * @param name table name
     * @return table if it exists
     */
    Optional<Table> findTableByName(String name);

    /**
     * Create table in the database
     *
     * @param name table name
     * @param scheme scheme
     * @return table
     */
    Table createTable(String name, Scheme scheme);

    /**
     * Return state
     *
     * @return state
     */
    State state();
}
