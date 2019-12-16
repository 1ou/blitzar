package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.connection.Server;
import io.toxa108.blitzar.storage.connection.impl.ServerImpl;
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

public class StorageApplication {

    public static void main(String[] args) {
        final Server server = new ServerImpl(9005);

        final FileManager fileManager = new FileManagerImpl();
        final DatabaseContext databaseContext = new DatabaseContextImpl(fileManager);
        final DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        final DatabaseManager databaseManager = new DatabaseManagerImpl(
                databaseContext, dataDefinitionQueryResolver);
        final QueryProcessor queryProcessor = new QueryProcessorImpl();
    }
}
