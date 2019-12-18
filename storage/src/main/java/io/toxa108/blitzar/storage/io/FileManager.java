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
     * @param scheme       scheme
     * @return result query
     */
    Table initializeTable(String databaseName, String tableName, Scheme scheme);

    /**
     * Load tables from disk to main memory
     *
     * @param databaseName databaseName
     * @return tables
     */
    List<Table> loadTables(String databaseName);

    /**
     * Load table from disk
     *
     * @param databaseName database name
     * @param tableName    table name
     * @return table
     */
    Table loadTable(String databaseName, String tableName);

    /**
     * Clear
     */
    void clear();
}
