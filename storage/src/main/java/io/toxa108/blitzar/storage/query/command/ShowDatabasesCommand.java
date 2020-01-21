package io.toxa108.blitzar.storage.query.command;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.query.UserContext;

import java.util.stream.Collectors;

public class ShowDatabasesCommand implements SqlCommand {
    private final DatabaseContext databaseContext;

    public ShowDatabasesCommand(DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
        return databaseContext.databases()
                .stream()
                .map(Database::name)
                .collect(Collectors.joining("\n"))
                .getBytes();
    }
}
