package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.command.SqlCommandFactory;
import io.toxa108.blitzar.storage.query.impl.BzDataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.impl.BzDataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.impl.SqlReservedWords;

import java.util.concurrent.ConcurrentHashMap;

public class BzSqlCommandFactory implements SqlCommandFactory {
    private final DatabaseContext databaseContext;
    private final ConcurrentHashMap<String, String> usersActiveDatabases;

    public BzSqlCommandFactory(final DatabaseContext databaseContext,
                               final ConcurrentHashMap<String, String> usersActiveDatabases) {
        this.databaseContext = databaseContext;
        this.usersActiveDatabases = usersActiveDatabases;
    }

    @Override
    public SqlCommand initializeCommand(final String[] parts) {
        final String errorKeyword = "error";
        final SqlReservedWords command = SqlReservedWords.valueOf(parts[0].toUpperCase());

        switch (command) {
            case CREATE:
                switch (SqlReservedWords.valueOf(parts[1].toUpperCase())) {
                    case TABLE:
                        return new CreateTableCommand(new BzDataDefinitionQueryResolver(databaseContext));
                    case DATABASE:
                        return new CreateDatabaseCommand(new BzDataDefinitionQueryResolver(databaseContext));
                    default:
                        return new ErrorCommand();
                }
            case INSERT:
                return new InsertToTableCommand(databaseContext, new BzDataManipulationQueryResolver(databaseContext));
            case SELECT:
                return new SelectFromTableCommand(databaseContext, new BzDataManipulationQueryResolver(databaseContext));
            case SHOW:
                switch (SqlReservedWords.valueOf(parts[1].toUpperCase())) {
                    case DATABASES:
                        return new ShowDatabasesCommand(databaseContext);
                    case TABLES:
                        return new ShowTablesCommand(databaseContext);
                    default:
                        return new ErrorCommand();
                }
            case USE:
                return new UseDatabaseCommand(usersActiveDatabases);
            case DELETE:
            default:
                return new ErrorCommand();
        }
    }
}
