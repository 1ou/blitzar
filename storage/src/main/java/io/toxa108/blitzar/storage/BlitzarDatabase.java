package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.DatabaseContextImpl;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.manager.DatabaseManagerImpl;
import io.toxa108.blitzar.storage.database.manager.user.UserManager;
import io.toxa108.blitzar.storage.database.manager.user.UserManagerImpl;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.DataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQueryResolverImpl;
import io.toxa108.blitzar.storage.query.impl.DataManipulationQueryResolverImpl;
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
        final FileManager fileManager;
        try {
            fileManager = new FileManagerImpl(path, databaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }
        final DatabaseContext databaseContext;
        try {
            databaseContext = new DatabaseContextImpl(fileManager);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }

        final DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        final UserManager userManager = new UserManagerImpl();

        final DataManipulationQueryResolver dataManipulationQueryResolver =
                new DataManipulationQueryResolverImpl(databaseContext);

        this.databaseManager = new DatabaseManagerImpl(userManager, dataDefinitionQueryResolver, dataManipulationQueryResolver);
        this.queryProcessor = new QueryProcessorImpl(databaseManager, databaseContext);
    }

    public DatabaseManager databaseManager() {
        return databaseManager;
    }

    public QueryProcessor queryProcessor() {
        return queryProcessor;
    }
}
