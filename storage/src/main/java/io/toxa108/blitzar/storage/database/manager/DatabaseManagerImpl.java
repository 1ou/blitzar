package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.QueryContext;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.*;

import java.util.Optional;

public class DatabaseManagerImpl implements DatabaseManager {
    private final DatabaseContext databaseContext;
    private final DataDefinitionQueryResolver dataDefinitionQueryResolver;

    public DatabaseManagerImpl(DatabaseContext databaseContext, DataDefinitionQueryResolver dataDefinitionQueryResolver) {
        this.databaseContext = databaseContext;
        this.dataDefinitionQueryResolver = dataDefinitionQueryResolver;
    }

    @Override
    public ResultQuery resolveDataDefinitionQuery(DataDefinitionQuery query) throws QueryProcessException {
        query.setQueryContext(createQueryContext(query));
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
                return new EmptyResultQuery();
        }
    }

    private QueryContext createQueryContext(AbstractQuery query) {
        Optional<Database> database = databaseContext.findByName(query.databaseName());
        if (database.isEmpty()) {
            database = Optional.of(databaseContext.createDatabase(query.databaseName()));
        }
        Optional<Table> table = database.get().findTableByName(query.tableName());
        if (table.isEmpty()) {
            table = Optional.of(database.get().createTable(query.tableName()));
        }
        return new QueryContextImpl(database.get(), table.get());
    }
}
