package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

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
     * @throws IOException disk io issue
     */
    Database createDatabase(String name) throws IOException;

    /**
     * Return databases
     * @return databases
     */
    Set<Database> databases();
}
