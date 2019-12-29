package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;

import java.util.HashSet;
import java.util.Set;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;

    public QueryProcessorImpl(@NotNull final DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] process(@NotNull final UserContext userContext, @NotNull final byte[] request) {
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
        DataDefinitionQuery dataDefinitionQuery;
        DataManipulationQuery dataManipulationQuery;

        switch (parts[0]) {
            case createKeyword:
                final String act = parts[1];
                switch (act) {
                    case tableKeyword:
                        return createTable(userContext, parts);
                    case databaseKeyword:
                        return createDatabase(userContext, parts);
                    default:
                        return errorKeyword.getBytes();
                }
            case useKeyword:
            case selectKeyword:
            case insertKeyword:
            case deleteKeyword:
            default:
                break;
        }

        return errorKeyword.getBytes();
    }

    private byte[] createTable(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final String name = sql[2];
        final Set<Field> fields = new HashSet<>();
        final Set<Index> indexes = new HashSet<>();

        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                userContext.databaseName(),
                name,
                Set.of(),
                Set.of(),
                DataDefinitionQuery.Type.CREATE_TABLE
        );

        return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
    }

    private byte[] createDatabase(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                sql[2], DataDefinitionQuery.Type.CREATE_DATABASE
        );
        return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
    }
}
