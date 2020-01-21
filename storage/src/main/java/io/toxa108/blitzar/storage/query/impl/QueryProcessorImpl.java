package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.impl.*;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;
    public final DatabaseContext databaseContext;

    public QueryProcessorImpl(@NotNull final DatabaseManager databaseManager,
                              @NotNull final DatabaseContext databaseContext) {
        this.databaseManager = databaseManager;
        this.databaseContext = databaseContext;
    }

    @Override
    public byte[] process(@NotNull final UserContext userContext,
                          @NotNull final byte[] request) {
        String query = new String(request).toLowerCase();
        final char endOfQuerySign = ';';
        final String splitQuerySign = " ";
        final String createKeyword = "create";
        final String selectKeyword = "select";
        final String insertKeyword = "insert";
        final String deleteKeyword = "delete";
        final String useKeyword = "use";
        final String tableKeyword = "table";
        final String databaseKeyword = "database";
        final String showKeyword = "show";
        final String errorKeyword = "error";

        if (query.charAt(query.length() - 1) != endOfQuerySign) {
            throw new IllegalArgumentException();
        } else {
            query = query.replaceAll(";", "");
        }

        final String[] parts = query.split(splitQuerySign);
        if (parts.length < 2) {
            throw new IllegalArgumentException();
        }

        final String act = parts[1];

        switch (parts[0]) {
            case createKeyword:
                switch (act) {
                    case tableKeyword:
                        return new CreateTableCommand(databaseManager).execute(userContext, parts);
                    case databaseKeyword:
                        return new CreateDatabaseCommand(databaseManager).execute(userContext, parts);
                    default:
                        return errorKeyword.getBytes();
                }
            case insertKeyword:
                return new InsertToTableCommand(databaseContext, databaseManager)
                        .execute(userContext, parts);
            case selectKeyword:
                return new SelectFromTableCommand(databaseContext, databaseManager)
                        .execute(userContext, parts);
            case showKeyword:
                switch (act) {
                    case "databases":
                        return new ShowDatabasesCommand(databaseContext).execute(userContext, parts);
                    case "tables":
                        return new ShowTablesCommand(databaseContext).execute(userContext, parts);
                }
            case useKeyword:
            case deleteKeyword:
            default:
                break;
        }

        return errorKeyword.getBytes();
    }
}