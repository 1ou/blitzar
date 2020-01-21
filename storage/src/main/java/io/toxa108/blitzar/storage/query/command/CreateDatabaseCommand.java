package io.toxa108.blitzar.storage.query.command;

import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;

public class CreateDatabaseCommand implements SqlCommand {
    private final DatabaseManager databaseManager;

    public CreateDatabaseCommand(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
        final DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                sql[2], DataDefinitionQuery.Type.CREATE_DATABASE
        );
        return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
    }
}
