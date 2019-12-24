package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.query.QueryProcessor;

public class QueryProcessorImpl implements QueryProcessor {
    public final DatabaseManager databaseManager;

    public QueryProcessorImpl(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] process(byte[] request) {
        final String query = new String(request).toLowerCase();
        final char endOfQuerySign = ';';
        final String splitQuerySign = " ";
        final String createKeyword = "create";
        final String selectKeyword = "select";
        final String insertKeyword = "insert";
        final String deleteKeyword = "delete";

        if (query.charAt(query.length() - 1) != endOfQuerySign) {
            throw new IllegalArgumentException();
        }

        String[] parts = query.split(splitQuerySign);
        if (parts.length == 0) {
            throw new IllegalArgumentException();
        }

        switch (parts[0]) {
            case createKeyword:
                DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery();
                return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery);
                break;
            case selectKeyword:
                break;
            case insertKeyword:
                break;
            case deleteKeyword:
                break;
            default:
                break;
        }
    }
}
