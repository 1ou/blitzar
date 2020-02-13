package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.context.impl.BzDatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.impl.BzDatabaseContext;
import io.toxa108.blitzar.storage.database.manager.user.BzUserManager;
import io.toxa108.blitzar.storage.database.manager.user.UserManager;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.BzFileManager;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.impl.BzDataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.impl.BzQueryProcessor;

import java.io.IOException;

/**
 * Timeseries database
 */
public class BlitzarDatabase {
    private final UserManager userManager;
    private final QueryProcessor queryProcessor;
    private final FileManager fileManager;
    private final DatabaseContext databaseContext;

    public BlitzarDatabase(@Nullable final String path) {
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

        final DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new BzDataDefinitionQueryResolver(databaseContext);

        this.userManager = new BzUserManager();
        this.queryProcessor = new BzQueryProcessor(databaseContext);
    }

    public UserManager userManager() {
        return userManager;
    }

    public void clear() {
        fileManager.clear();
        userManager.clear();
        databaseContext.databases().clear();
    }

    public QueryProcessor queryProcessor() {
        return queryProcessor;
    }
}
