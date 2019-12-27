package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.DatabaseContextImpl;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.manager.DatabaseManagerImpl;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQueryResolverImpl;
import io.toxa108.blitzar.storage.query.impl.QueryProcessorImpl;

import java.io.IOException;

public class BlitzarDatabase {
    private final DatabaseManager databaseManager;
    private final QueryProcessor queryProcessor;

    public BlitzarDatabase(final String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException();
        }

        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(16);
        final FileManager fileManager = new FileManagerImpl(path, databaseConfiguration);
        final DatabaseContext databaseContext;
        try {
            databaseContext = new DatabaseContextImpl(fileManager);
        } catch (IOException e) {
            throw new IllegalStateException();
        }

        final DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        this.databaseManager = new DatabaseManagerImpl(dataDefinitionQueryResolver);
        this.queryProcessor = new QueryProcessorImpl(databaseManager);
    }

    public DatabaseManager databaseManager() {
        return databaseManager;
    }

    public QueryProcessor queryProcessor() {
        return queryProcessor;
    }
}
