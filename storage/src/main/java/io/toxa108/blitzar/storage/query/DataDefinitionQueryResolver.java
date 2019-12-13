package io.toxa108.blitzar.storage.query;

import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;

public interface DataDefinitionQueryResolver {
    ResultQuery createDatabase(DataDefinitionQuery query);
    ResultQuery createTable(DataDefinitionQuery query);
    ResultQuery createIndex(DataDefinitionQuery query);
    ResultQuery dropDatabase(DataDefinitionQuery query);
    ResultQuery dropTable(DataDefinitionQuery query);
    ResultQuery dropIndex(DataDefinitionQuery query);
}
