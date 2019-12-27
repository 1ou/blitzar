package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.impl.SchemeImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

import java.io.IOException;
import java.util.Optional;

public class DataDefinitionQueryResolverImpl implements DataDefinitionQueryResolver {
    private final DatabaseContext databaseContext;

    public DataDefinitionQueryResolverImpl(@NotNull final DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public ResultQuery createDatabase(@NotNull final DataDefinitionQuery query) {
        try {
            Database database = databaseContext.createDatabase(query.databaseName());
            return new EmptySuccessResultQuery();
        } catch (IOException e) {
            e.printStackTrace();
            return new ErrorResultQuery();
        }
    }

    @Override
    public ResultQuery createTable(@NotNull final DataDefinitionQuery query) {
        Optional<Database> databaseOptional = databaseContext.findByName(query.databaseName());
        if (databaseOptional.isEmpty()) {
            return new ErrorResultQuery();
        } else {
            Database database = databaseOptional.get();
            try {
                database.createTable(query.tableName(), new SchemeImpl(query.fields(), query.getIndexes()));
                return new EmptySuccessResultQuery();
            } catch (IOException e) {
                e.printStackTrace();
                return new ErrorResultQuery();
            }
        }
    }

    @Override
    public ResultQuery createIndex(@NotNull final DataDefinitionQuery query) {
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
