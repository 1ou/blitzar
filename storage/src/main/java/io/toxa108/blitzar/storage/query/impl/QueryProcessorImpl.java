package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.impl.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;
    public final DatabaseContext databaseContext;
    private Pattern patternQuery = Pattern.compile("^[a-zA-Z0-9_; ]*$");
    private final Semaphore semaphore;

    /**
     * key - login
     * value - database name
     */
    private final ConcurrentHashMap<String, String> usersActiveDatabases;

    public QueryProcessorImpl(final DatabaseManager databaseManager,
                              final DatabaseContext databaseContext) {
        this.databaseManager = databaseManager;
        this.databaseContext = databaseContext;
        this.usersActiveDatabases = new ConcurrentHashMap<>();
        this.semaphore = new Semaphore(50, true);
    }

    @Override
    public byte[] process(UserContext userContext,
                          final byte[] request) {
        semaphore.tryAcquire();

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

        final SqlReservedWords command = SqlReservedWords.valueOf(parts[0].toUpperCase());

        switch (command) {
            case CREATE:
                switch (SqlReservedWords.valueOf(parts[1].toUpperCase())) {
                    case TABLE:
                        return new CreateTableCommand(databaseManager).execute(userContext, parts);
                    case DATABASE:
                        return new CreateDatabaseCommand(databaseManager).execute(userContext, parts);
                    default:
                        return errorKeyword.getBytes();
                }
            case INSERT:
                return new InsertToTableCommand(databaseContext, databaseManager)
                        .execute(userContext, parts);
            case SELECT:
                return new SelectFromTableCommand(databaseContext, databaseManager)
                        .execute(userContext, parts);
            case SHOW:
                switch (SqlReservedWords.valueOf(parts[1].toUpperCase())) {
                    case DATABASES:
                        return new ShowDatabasesCommand(databaseContext).execute(userContext, parts);
                    case TABLES:
                        return new ShowTablesCommand(databaseContext).execute(userContext, parts);
                    default:
                        return errorKeyword.getBytes();
                }
            case USE:
                usersActiveDatabases.put(userContext.user().login(), parts[1]);
                break;
            case DELETE:
            default:
                break;
        }

        semaphore.release();
        return errorKeyword.getBytes();
    }

    String optimizeQuery(final byte[] request) {
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