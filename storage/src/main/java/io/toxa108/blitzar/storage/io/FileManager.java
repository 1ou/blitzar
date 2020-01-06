package io.toxa108.blitzar.storage.io;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.io.IOException;
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
     * @throws IOException disk io exception
     */
    Database initializeDatabase(String name) throws IOException;

    /**
     * Initialize table
     *
     * @param databaseName database
     * @param tableName    name
     * @param scheme       scheme
     * @return result query
     * @throws IOException disk io exception
     */
    Table initializeTable(String databaseName, String tableName, Scheme scheme) throws IOException;

    /**
     * Load tables from disk to main memory
     *
     * @param databaseName databaseName
     * @return tables
     * @throws IOException disk io exception
     */
    List<Table> loadTables(String databaseName) throws IOException;

    /**
     * Load table from disk
     *
     * @param databaseName database name
     * @param tableName    table name
     * @return table
     * @throws IOException disk io exception
     */
    Table loadTable(String databaseName, String tableName) throws IOException;

    /**
     * Clear
     */
    void clear();
}
