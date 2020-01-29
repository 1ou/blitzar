package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;

import java.util.stream.Collectors;

public class ShowDatabasesCommand implements SqlCommand {
    private final DatabaseContext databaseContext;

    public ShowDatabasesCommand(final DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        return databaseContext.databases()
                .stream()
                .map(Database::name)
                .collect(Collectors.joining("\n"))
                .getBytes();
    }
}
