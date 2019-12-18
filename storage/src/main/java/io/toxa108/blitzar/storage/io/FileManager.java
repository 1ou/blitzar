package io.toxa108.blitzar.storage.io;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.io.File;
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
     * Save table scheme
     *
     * @param file   file
     * @param scheme table scheme
     */
    void saveTableScheme(File file, Scheme scheme);

    /**
     * Load table scheme
     *
     * @param file file
     * @return scheme
     */
    Scheme loadTableScheme(File file);

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
