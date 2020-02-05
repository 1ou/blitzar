package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;

public class DeleteFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;

    public DeleteFromTableCommand(final DatabaseContext databaseContext, final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        return new byte[0];
    }
}
