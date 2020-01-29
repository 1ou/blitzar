package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;

import java.util.stream.Collectors;

public class ShowTablesCommand implements SqlCommand {
    private final DatabaseContext databaseContext;

    public ShowTablesCommand(final DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        return databaseContext.databases()
                .stream()
                .filter(it -> it.name().equalsIgnoreCase(userContext.databaseName()))
                .flatMap(it -> it.tables().stream())
                .map(Table::name)
                .collect(Collectors.joining("\n"))
                .getBytes();
    }
}
