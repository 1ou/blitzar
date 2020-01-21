package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.Optional;

public class SelectFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;

    public SelectFromTableCommand(@NotNull final DatabaseContext databaseContext,
                                  @NotNull final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final Optional<Database> databaseOptional = databaseContext.findByName(userContext.databaseName());
        final String tableName = sql[3];

        if (databaseOptional.isPresent()) {
            final Optional<Table> tableOptional = databaseOptional.get().findTableByName(tableName);
            if (tableOptional.isPresent()) {
                final Table table = tableOptional.get();
                final Scheme scheme = table.scheme();
                table.search();
            }
        }
        return new ErrorResultQuery().toBytes();
    }
}
