package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;

public class CreateIndexCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;

    public CreateIndexCommand(final DatabaseContext databaseContext,
                              final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        return new byte[0];
    }
}
