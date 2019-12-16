package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;

import java.util.List;
import java.util.Optional;

public interface DatabaseContext {
    /**
     * Find database by name
     * @param database database name
     * @return database
     */
    Optional<Database> findByName(String database);

    /**
     * Create database
     * @param name name
     * @return database
     */
    Database createDatabase(String name);

    /**
     * Return databases
     * @return databases
     */
    List<Database> databases();
}
