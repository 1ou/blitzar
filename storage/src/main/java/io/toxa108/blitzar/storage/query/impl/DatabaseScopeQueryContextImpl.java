package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.QueryContext;

public class DatabaseScopeQueryContextImpl implements QueryContext {
    private final Database database;

    public DatabaseScopeQueryContextImpl(Database database) {
        this.database = database;
    }

    @Override
    public Database database() {
        return database;
    }

    @Override
    public Table table() {
        throw new UnsupportedOperationException("Database scope doesn't support the table");
    }
}
