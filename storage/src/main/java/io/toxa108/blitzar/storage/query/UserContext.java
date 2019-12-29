package io.toxa108.blitzar.storage.query;

import io.toxa108.blitzar.storage.database.manager.user.User;

public interface UserContext {
    /**
     * Return database name
     * @return database name
     */
    String databaseName();

    /**
     * Return current user
     * @return current user
     */
    User user();
}
