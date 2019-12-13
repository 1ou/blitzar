package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.schema.impl.SchemeImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

import java.util.List;

public class DataDefinitionQueryResolverImpl implements DataDefinitionQueryResolver {
    @Override
    public ResultQuery createDatabase(DataDefinitionQuery query) {
        return new EmptyResultQuery();
    }

    @Override
    public ResultQuery createTable(DataDefinitionQuery query) {
        return query.context().table().initializeScheme(
                new SchemeImpl(query.fields(), List.of()));
    }

    @Override
    public ResultQuery createIndex(DataDefinitionQuery query) {
        return new EmptyResultQuery();
    }

    @Override
    public ResultQuery dropDatabase(DataDefinitionQuery query) {
        return new EmptyResultQuery();
    }

    @Override
    public ResultQuery dropTable(DataDefinitionQuery query) {
        return new EmptyResultQuery();
    }

    @Override
    public ResultQuery dropIndex(DataDefinitionQuery query) {
        return new EmptyResultQuery();
    }
}
