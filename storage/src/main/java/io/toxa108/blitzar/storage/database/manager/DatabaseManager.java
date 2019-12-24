package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.QueryProcessException;

public interface DatabaseManager {
    /**
     * Apply query to the database
     *
     * @param query DDL query
     * @return result
     * @throws QueryProcessException exception during process query
     */
    ResultQuery resolveDataDefinitionQuery(DataDefinitionQuery query) throws QueryProcessException;
}
