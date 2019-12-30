package io.toxa108.blitzar.storage.query;

import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;

public interface DataDefinitionQueryResolver {
    /**
     * Create database
     *
     * @param query query
     * @return result
     */
    ResultQuery createDatabase(DataDefinitionQuery query);

    /**
     * Create table
     *
     * @param query query
     * @return result
     */
    ResultQuery createTable(DataDefinitionQuery query);

    /**
     * Create index
     *
     * @param query query
     * @return result
     */
    ResultQuery createIndex(DataDefinitionQuery query);

    /**
     * Drop database
     *
     * @param query query
     * @return result
     */
    ResultQuery dropDatabase(DataDefinitionQuery query);

    /**
     * Drop table
     *
     * @param query query
     * @return result
     */
    ResultQuery dropTable(DataDefinitionQuery query);

    /**
     * Drop index
     *
     * @param query query
     * @return result
     */
    ResultQuery dropIndex(DataDefinitionQuery query);
}
