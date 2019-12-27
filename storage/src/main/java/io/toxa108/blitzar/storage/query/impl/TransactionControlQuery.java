package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;

public class TransactionControlQuery extends AbstractQuery {
    public TransactionControlQuery(@NotNull final String databaseName, @NotNull final String tableName) {
        super(databaseName, tableName);
    }
}
