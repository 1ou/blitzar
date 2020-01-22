package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.impl.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;
    public final DatabaseContext databaseContext;
    private Pattern patternQuery = Pattern.compile("^[a-zA-Z0-9_; ]*$");

    /**
     * key - login
     * value - database name
     */
    private final ConcurrentHashMap<String, String> usersActiveDatabases;

    public QueryProcessorImpl(@NotNull final DatabaseManager databaseManager,
                              @NotNull final DatabaseContext databaseContext) {
        this.databaseManager = databaseManager;
        this.databaseContext = databaseContext;
        this.usersActiveDatabases = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] process(@NotNull UserContext userContext,
                          @NotNull final byte[] request) {
        String contextDatabaseName = usersActiveDatabases.get(userContext.user().login());
        if (contextDatabaseName != null) {
            userContext = new UserContextImpl(
                    contextDatabaseName,
                    userContext.user()
            );
        }

        String query = optimizeQuery(request);
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
        final short minParts = 2;

        if (query.charAt(query.length() - 1) != endOfQuerySign) {
            throw new IllegalArgumentException();
        } else {
            query = query.replaceAll(";", "");
        }

        final String[] parts = query.split(splitQuerySign);
        if (parts.length < minParts) {
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
                    default:
                        return errorKeyword.getBytes();
                }
            case useKeyword:
                usersActiveDatabases.put(userContext.user().login(), parts[1]);
                break;
            case deleteKeyword:
            default:
                break;
        }

        return errorKeyword.getBytes();
    }

    String optimizeQuery(@NotNull final byte[] request) {
        final String query = new String(request).toLowerCase();
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < query.length(); ++i) {
            final char c = query.charAt(i);
            try {
                if (!patternQuery.matcher(Character.toString(c)).matches()) {
                    stringBuilder.append(" ")
                            .append(c)
                            .append(" ");
                } else {
                    stringBuilder.append(c);
                }
            } catch (PatternSyntaxException exception) {
                stringBuilder.append(" ")
                        .append(c)
                        .append(" ");
            }
        }
        return stringBuilder.toString().replaceAll(" {2,}", " ");
    }
}