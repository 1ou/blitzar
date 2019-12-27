package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;

public class DataManipulationQuery extends AbstractQuery {
    public DataManipulationQuery(@NotNull final String databaseName, @NotNull final String tableName) {
        super(databaseName, tableName);
    }
}
