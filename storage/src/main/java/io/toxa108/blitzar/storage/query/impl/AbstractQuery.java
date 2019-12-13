package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.query.Query;
import io.toxa108.blitzar.storage.query.QueryContext;

public class AbstractQuery implements Query {
    private final String database;
    private final String table;
    private QueryContext queryContext;

    public AbstractQuery(String database, String table) {
        this.database = database;
        this.table = table;
    }

    @Override
    public String database() {
        return database;
    }

    @Override
    public String table() {
        return table;
    }

    public QueryContext context() {
        return queryContext;
    }

    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }
}
