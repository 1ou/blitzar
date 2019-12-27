package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;

public class DatabaseManagerImpl implements DatabaseManager {
    private final DataDefinitionQueryResolver dataDefinitionQueryResolver;

    public DatabaseManagerImpl(@NotNull DataDefinitionQueryResolver dataDefinitionQueryResolver) {
        this.dataDefinitionQueryResolver = dataDefinitionQueryResolver;
    }

    @Override
    public ResultQuery resolveDataDefinitionQuery(@NotNull DataDefinitionQuery query) {
        switch (query.type()) {
            case CREATE_DATABASE:
                return dataDefinitionQueryResolver.createDatabase(query);
            case CREATE_INDEX:
                return dataDefinitionQueryResolver.createIndex(query);
            case CREATE_TABLE:
                return dataDefinitionQueryResolver.createTable(query);
            case DROP_DATABASE:
                return dataDefinitionQueryResolver.dropDatabase(query);
            default:
                return new EmptySuccessResultQuery();
        }
    }
}
