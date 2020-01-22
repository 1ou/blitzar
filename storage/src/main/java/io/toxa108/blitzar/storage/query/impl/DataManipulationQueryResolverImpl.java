package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.RowImpl;
import io.toxa108.blitzar.storage.query.DataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;

public class DataManipulationQueryResolverImpl implements DataManipulationQueryResolver {
    private final DatabaseContext context;

    public DataManipulationQueryResolverImpl(@NotNull final DatabaseContext context) {
        this.context = context;
    }

    @Override
    public ResultQuery insert(DataManipulationQuery query) {
        Table table = context.findByName(query.databaseName())
                .orElseThrow()
                .findTableByName(query.tableName())
                .orElseThrow();

        final Key key = Key.fromField(table.scheme().primaryIndexField());
        table.addRow(new RowImpl(key, query.fields()));

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
