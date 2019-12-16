package io.toxa108.blitzar.storage.io;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.List;

public interface FileManager {
    /**
     * Get databases
     * @return databases
     */
    List<String> databases();

    /**
     * Initialize database
     * @param name name
     * @return result query
     */
    Database initializeDatabase(String name);

    /**
     * Initialize tablr
     * @param database database
     * @param name name
     * @return result query
     */
    Table initializeTable(Database database, String name);

    /**
     * Load tables from disk to main memory
     * @param databaseName databaseName
     * @return tables
     */
    List<Table> loadTables(String databaseName);

    /**
     * Clear
     */
    void clear();
}
