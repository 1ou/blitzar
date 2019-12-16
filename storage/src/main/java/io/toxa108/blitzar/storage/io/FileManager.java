package io.toxa108.blitzar.storage.io;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.List;

public interface FileManager {
    /**
     * Get databases
     *
     * @return databases
     */
    List<String> databases();

    /**
     * Initialize database
     *
     * @param name name
     * @return result query
     */
    Database initializeDatabase(String name);

    /**
     * Initialize table
     *
     * @param databaseName database
     * @param tableName    name
     * @return result query
     */
    Table initializeTable(String databaseName, String tableName);

    /**
     * Save table scheme
     *
     * @param databaseName database
     * @param tableName    name
     * @param scheme       table scheme
     * @return result query
     */
    Table saveTableScheme(String databaseName, String tableName, Scheme scheme);

    /**
     * Load table scheme
     *
     * @param databaseName database name
     * @param tableName    table name
     * @return scheme
     */
    Scheme loadTableScheme(String databaseName, String tableName);

    /**
     * Load tables from disk to main memory
     *
     * @param databaseName databaseName
     * @return tables
     */
    List<Table> loadTables(String databaseName);

    /**
     * Clear
     */
    void clear();
}
