package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.impl.SchemeImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

import java.util.List;

public class DataDefinitionQueryResolverImpl implements DataDefinitionQueryResolver {
    private final DatabaseContext databaseContext;

    public DataDefinitionQueryResolverImpl(DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public ResultQuery createDatabase(DataDefinitionQuery query) {
        Database database = databaseContext.createDatabase(query.databaseName());
        return new EmptySuccessResultQuery();
//бутина 78 кв 74
    }

    @Override
    public ResultQuery createTable(DataDefinitionQuery query) {
        return query.context().table().initializeScheme(
                new SchemeImpl(query.fields(), List.of()));
    }

    @Override
    public ResultQuery createIndex(DataDefinitionQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery dropDatabase(DataDefinitionQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery dropTable(DataDefinitionQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery dropIndex(DataDefinitionQuery query) {
        return new EmptySuccessResultQuery();
    }
}
