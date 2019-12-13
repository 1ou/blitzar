package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.schema.impl.SchemeImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

import java.util.List;

public class DataDefinitionQueryResolverImpl implements DataDefinitionQueryResolver {
    @Override
    public ResultQuery createDatabase(DataDefinitionQuery query) {
        return null;
    }

    @Override
    public ResultQuery createTable(DataDefinitionQuery query) {
        query.context().table().initializeScheme(
                new SchemeImpl(query.fields(), List.of()));

        return null;
    }

    @Override
    public ResultQuery createIndex(DataDefinitionQuery query) {
        return null;
    }

    @Override
    public ResultQuery dropDatabase(DataDefinitionQuery query) {
        return null;
    }

    @Override
    public ResultQuery dropTable(DataDefinitionQuery query) {
        return null;
    }

    @Override
    public ResultQuery dropIndex(DataDefinitionQuery query) {
        return null;
    }
}
