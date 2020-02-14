package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.query.OptimizeQuery;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.command.impl.BzSqlCommandFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Query processor
 */
public class BzQueryProcessor implements QueryProcessor {
    public final DatabaseContext databaseContext;
    private final OptimizeQuery optimizeQuery;
    private final Semaphore semaphore;

    /**
     * key - login
     * value - database name
     */
    private final ConcurrentHashMap<String, String> usersActiveDatabases;

    public BzQueryProcessor(final DatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
        this.usersActiveDatabases = new ConcurrentHashMap<>();
        this.semaphore = new Semaphore(50, true);
        this.optimizeQuery = new BzOptimizeQuery();
    }

    @Override
    public byte[] process(UserContext userContext,
                          final byte[] request) {
        semaphore.tryAcquire();

        String contextDatabaseName = usersActiveDatabases.get(userContext.user().login());
        if (contextDatabaseName != null) {
            userContext = new BzUserContext(
                    contextDatabaseName,
                    userContext.user()
            );
        }

        String query = optimizeQuery.optimize(new String(request));
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

        SqlCommand sqlCommand = new BzSqlCommandFactory(databaseContext, usersActiveDatabases).initializeCommand(parts);
        byte[] sqlQueryResultBytes = sqlCommand.execute(userContext, parts);

        semaphore.release();
        return sqlQueryResultBytes;
    }
}