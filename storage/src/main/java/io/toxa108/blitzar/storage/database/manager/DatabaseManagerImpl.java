package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.manager.user.UserManager;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.DataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.DataManipulationQuery;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

public class DatabaseManagerImpl implements DatabaseManager {
    private final UserManager userManager;
    private final DataDefinitionQueryResolver dataDefinitionQueryResolver;
    private final DataManipulationQueryResolver dataManipulationQueryResolver;

    public DatabaseManagerImpl(final UserManager userManager,
                               final DataDefinitionQueryResolver dataDefinitionQueryResolver,
                               final DataManipulationQueryResolver dataManipulationQueryResolver) {
        this.userManager = userManager;
        this.dataDefinitionQueryResolver = dataDefinitionQueryResolver;
        this.dataManipulationQueryResolver = dataManipulationQueryResolver;
    }

    @Override
    public ResultQuery resolveDataDefinitionQuery(DataDefinitionQuery query) {
        switch (query.type()) {
            case CREATE_DATABASE:
                return dataDefinitionQueryResolver.createDatabase(query);
            case CREATE_INDEX:
                return dataDefinitionQueryResolver.createIndex(query);
            case CREATE_TABLE:
                return dataDefinitionQueryResolver.createTable(query);
            case DROP_DATABASE:
                return dataDefinitionQueryResolver.dropDatabase(query);
            default:
                return new ErrorResultQuery();
        }
    }

    @Override
    public ResultQuery resolveDataManipulationQuery(DataManipulationQuery query) {
        switch (query.type()) {
            case INSERT:
                return dataManipulationQueryResolver.insert(query);
            case DELETE:
                return dataManipulationQueryResolver.delete(query);
            case UPDATE:
                return dataManipulationQueryResolver.update(query);
            case SELECT:
                return dataManipulationQueryResolver.select(query);
            default:
                return new ErrorResultQuery();
        }
    }

    @Override
    public UserManager userManager() {
        return userManager;
    }
}
