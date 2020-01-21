package io.toxa108.blitzar.storage.query.command;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.UserContext;

import java.util.stream.Collectors;

public class ShowTablesCommand implements SqlCommand {
    private final DatabaseContext databaseContext;

    public ShowTablesCommand(DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
        return databaseContext.databases()
                .stream()
                .filter(it -> it.name().equalsIgnoreCase(userContext.databaseName()))
                .flatMap(it -> it.tables().stream())
                .map(Table::name)
                .collect(Collectors.joining("\n"))
                .getBytes();
    }
}
