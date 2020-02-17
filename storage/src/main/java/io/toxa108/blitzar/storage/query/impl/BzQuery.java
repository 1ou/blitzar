package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.query.Query;
import io.toxa108.blitzar.storage.query.QueryContext;

public abstract class BzQuery implements Query {
    private final String databaseName;
    private final String tableName;
    private QueryContext queryContext;

    public BzQuery(final String databaseName,
                   final String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    public QueryContext context() {
        return queryContext;
    }

    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }
}
