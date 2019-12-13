package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.query.Query;
import io.toxa108.blitzar.storage.query.ResultQuery;

public class DatabaseManagerImpl implements DatabaseManager {
    private final DatabaseContext databaseContext;

    public DatabaseManagerImpl(DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public ResultQuery resolveQuery(Query query) {
        return null;
    }
}
