package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.query.DataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

public class DataManipulationQueryResolverImpl implements DataManipulationQueryResolver {
    private final DatabaseContext databaseContext;

    public DataManipulationQueryResolverImpl(@NotNull final DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public ResultQuery insert(DataManipulationQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery update(DataManipulationQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery delete(DataManipulationQuery query) {
        return new EmptySuccessResultQuery();
    }

    @Override
    public ResultQuery select(DataManipulationQuery query) {
        return new EmptySuccessResultQuery();
    }
}
