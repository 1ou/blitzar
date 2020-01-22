package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.State;

import java.io.IOException;
import java.util.List;
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
     * @param name   table name
     * @param scheme scheme
     * @return table
     * @throws IOException disk io issue
     */
    Table createTable(String name, Scheme scheme) throws IOException;

    /**
     * Tables
     *
     * @return list of tables in the database
     */
    List<Table> tables();

    /**
     * Return state
     *
     * @return state
     */
    State state();
}
