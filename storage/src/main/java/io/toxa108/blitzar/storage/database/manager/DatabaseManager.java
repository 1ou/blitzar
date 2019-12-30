package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.manager.user.UserManager;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.DataManipulationQuery;

public interface DatabaseManager {
    /**
     * Apply data definition query to the database
     *
     * @param query DDL query
     * @return result query
     */
    ResultQuery resolveDataDefinitionQuery(DataDefinitionQuery query);

    /**
     * Apply data manipulation query to the table
     *
     * @param query DML query
     * @return result query
     */
    ResultQuery resolveDataManipulationQuery(DataManipulationQuery query);

    /**
     * Get user manager
     *
     * @return user manager
     */
    UserManager userManager();
}
