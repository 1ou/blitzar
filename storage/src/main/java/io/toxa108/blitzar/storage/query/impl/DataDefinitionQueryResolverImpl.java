package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.impl.SchemeImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

import java.util.Optional;

public class DataDefinitionQueryResolverImpl implements DataDefinitionQueryResolver {
    private final DatabaseContext databaseContext;

    public DataDefinitionQueryResolverImpl(DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public ResultQuery createDatabase(DataDefinitionQuery query) {
        Database database = databaseContext.createDatabase(query.databaseName());
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery createTable(DataDefinitionQuery query) {
        Optional<Database> databaseOptional = databaseContext.findByName(query.databaseName());
        if (databaseOptional.isEmpty()) {
            return new ErrorResultQuery();
        } else {
            Database database = databaseOptional.get();
            database.createTable(query.tableName(), new SchemeImpl(query.fields(), query.getIndices()));
            return new EmptySuccessResultQuery();
        }
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
