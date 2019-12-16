package io.toxa108.blitzar.storage.database.schema;

import java.util.Optional;

public interface Database {
    /**
     * Return database name
     * @return database name
     */
    String name();

    /**
     * Find table by name
     * @param name table name
     * @return table if it exists
     */
    Optional<Table> findTableByName(String name);

    /**
     * Create table in the database
     * @param name table name
     * @return table
     */
    Table createTable(String name);
}
