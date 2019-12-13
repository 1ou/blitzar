package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.QueryContext;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.AbstractQuery;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.QueryContextImpl;
import io.toxa108.blitzar.storage.query.impl.QueryProcessException;

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
            case DROP_DATABASE:
            default:
                return null;
        }
    }

    private QueryContext createQueryContext(AbstractQuery query) {
        Database database = databaseContext.findByName(query.database());
        Optional<Table> table = database.findTableByName(query.table());
        if (table.isEmpty()) {
            table = Optional.of(database.createTable());
        }
        return new QueryContextImpl(database, table.get());
    }
}
