package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.Byteble;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.Optional;

public class SelectFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;
    private final ArrayManipulator arrayManipulator;

    public SelectFromTableCommand(@NotNull final DatabaseContext databaseContext,
                                  @NotNull final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
        this.arrayManipulator = new ArrayManipulator();
    }

    @Override
    public byte[] execute(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final Optional<Database> databaseOptional = databaseContext.findByName(userContext.databaseName());
        final String tableName = sql[3];

        if (databaseOptional.isPresent()) {
            final Optional<Table> tableOptional = databaseOptional.get().findTableByName(tableName);
            if (tableOptional.isPresent()) {
                final Table table = tableOptional.get();
                return table.search()
                        .stream()
                        .map(it -> {
                            byte[] bytes = ((Byteble) it).toBytes();
                            return new String(bytes) + "\n";
                        })
                        .reduce((l, r) -> l + r)
                        .orElse("")
                        .getBytes();
            }
        }
        return new ErrorResultQuery().toBytes();
    }
}
