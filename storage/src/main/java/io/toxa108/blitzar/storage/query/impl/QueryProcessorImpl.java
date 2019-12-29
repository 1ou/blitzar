package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        boolean startFields = false;
        List<String> fieldsSemantic = new ArrayList<>();
        int p = 0;
        for (int i = 0; i < sql.length; ++i) {
            if ("(".equals(sql[i])) {
                startFields = true;
                p = i + 1;
            }

            if (startFields) {
                if (sql[i].contains(",") || ")".equals(sql[i])) {
                    int k = i;
                    if (")".equals(sql[i])) {
                        k--;
                    }
                    StringBuilder fieldBuilder = new StringBuilder();
                    for (int y = p; y <= k; ++y) {
                        fieldBuilder.append(sql[y].replaceAll("(,| |\\)|)", ""));
                        if (y != k) {
                            fieldBuilder.append(" ");
                        }
                    }

                    fieldsSemantic.add(fieldBuilder.toString());
                    p = i + 1;
                }
            }
        }

        for (String s : fieldsSemantic) {
            final String[] parts = s.split(" ");
            FieldType type;
            switch (parts[1]) {
                case "short":
                    type = FieldType.SHORT;
                    break;
                case "int":
                    type = FieldType.INTEGER;
                    break;
                case "long":
                    type = FieldType.LONG;
                    break;
                case "varchar":
                    type = FieldType.VARCHAR;
                    break;
                default:
                    return new ErrorResultQuery().toBytes();
            }
            Nullable nullable = Nullable.NULL;
            if ("not".equals(parts[2]) && "null".equals(parts[3])) {
                nullable = Nullable.NOT_NULL;
            }
            Unique unique = Unique.NOT_UNIQUE;
            if (parts.length >= 5 && "primary".equals(parts[4])) {
                unique = Unique.UNIQUE;
            }
            if (s.contains("key")) {
                Index index = new IndexImpl(Set.of(parts[0]), IndexType.PRIMARY);
                indexes.add(index);
            }

            Field field = new FieldImpl(parts[0], type, nullable, unique, new byte[type.size()]);
            fields.add(field);
        }
        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                userContext.databaseName(),
                name,
                fields,
                indexes,
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
