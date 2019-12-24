package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;

import java.util.Set;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;

    public QueryProcessorImpl(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] process(byte[] request) {
        String query = new String(request).toLowerCase();
        final char endOfQuerySign = ';';
        final String splitQuerySign = " ";
        final String createKeyword = "create";
        final String selectKeyword = "select";
        final String insertKeyword = "insert";
        final String deleteKeyword = "delete";
        final String tableKeyword = "table";
        final String databaseKeyword = "database";
        final String errorKeyword = "error";
        if (query.charAt(query.length() - 1) != endOfQuerySign) {
            throw new IllegalArgumentException();
        } else {
            query = query.replaceAll(";", "");
        }

        final String[] parts = query.split(splitQuerySign);
        if (parts.length < 3) {
            throw new IllegalArgumentException();
        }

        switch (parts[0]) {
            case createKeyword:
                final String act = parts[1];
                final String name = parts[2];
                DataDefinitionQuery.Type type;
                DataDefinitionQuery dataDefinitionQuery;

                switch (act) {
                    case tableKeyword:
                        dataDefinitionQuery = new DataDefinitionQuery(
                            "database",
                                name,
                                Set.of(),
                                Set.of(),
                                DataDefinitionQuery.Type.CREATE_TABLE
                        );
                        break;
                    case databaseKeyword:
                        dataDefinitionQuery = new DataDefinitionQuery(
                            name, DataDefinitionQuery.Type.CREATE_DATABASE
                        );
                        break;
                    default:
                        return errorKeyword.getBytes();
                }

                return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
            case selectKeyword:
            case insertKeyword:
            case deleteKeyword:
            default:
                break;
        }

        return errorKeyword.getBytes();
    }
}
