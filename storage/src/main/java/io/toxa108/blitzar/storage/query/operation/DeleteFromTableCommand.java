package io.toxa108.blitzar.storage.query.operation;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.UserContext;

public class DeleteFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;

    public DeleteFromTableCommand(DatabaseContext databaseContext, DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
        return new byte[0];
    }
}
