package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.context.impl.BzDatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.impl.BzDatabaseContext;
import io.toxa108.blitzar.storage.database.manager.user.BzUsers;
import io.toxa108.blitzar.storage.database.manager.user.Users;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.BzFileManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.impl.BzQueryProcessor;

import java.io.IOException;

/**
 * Timeseries database
 */
public class BzDatabase {
    private final Users users;
    private final QueryProcessor queryProcessor;
    private final FileManager fileManager;
    private final DatabaseContext databaseContext;

    public BzDatabase(@Nullable final String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException();
        }

        final DatabaseConfiguration databaseConfiguration = new BzDatabaseConfiguration(16);
        try {
            fileManager = new BzFileManager(path, databaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }

        try {
            databaseContext = new BzDatabaseContext(fileManager);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }

        this.users = new BzUsers();
        this.queryProcessor = new BzQueryProcessor(databaseContext);
    }

    public BzDatabase(@Nullable final String path,
                      final DatabaseConfiguration databaseConfiguration) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException();
        }

        try {
            fileManager = new BzFileManager(path, databaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }

        try {
            databaseContext = new BzDatabaseContext(fileManager);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }

        this.users = new BzUsers();
        this.queryProcessor = new BzQueryProcessor(databaseContext);
    }

    public Users userManager() {
        return users;
    }

    public void clear() {
        fileManager.clear();
        users.clear();
        databaseContext.databases().clear();
    }

    public QueryProcessor queryProcessor() {
        return queryProcessor;
    }
}
