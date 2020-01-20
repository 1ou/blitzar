package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.*;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.query.QueryProcessor;
import io.toxa108.blitzar.storage.query.UserContext;

import java.util.*;
import java.util.stream.Collectors;

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
            case insertKeyword:
                return insertIntoTable(userContext, parts);
            case selectKeyword:
                return selectFromTable(userContext, parts);
            case useKeyword:
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
        final List<String> fieldsSemantic = new ArrayList<>();

        boolean startFields = false;
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

            final Field field = new FieldImpl(parts[0], type, nullable, unique, new byte[type.size()]);
            fields.add(field);
        }
        final DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                userContext.databaseName(),
                name,
                fields,
                indexes,
                DataDefinitionQuery.Type.CREATE_TABLE
        );

        return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
    }

    private byte[] createDatabase(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                sql[2], DataDefinitionQuery.Type.CREATE_DATABASE
        );
        return databaseManager.resolveDataDefinitionQuery(dataDefinitionQuery).toBytes();
    }

    private byte[] insertIntoTable(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final Optional<Database> databaseOptional = databaseContext.findByName(userContext.databaseName());
        final String tableName = sql[2];

        if (databaseOptional.isPresent()) {
            final Optional<Table> tableOptional = databaseOptional.get().findTableByName(tableName);
            if (tableOptional.isPresent()) {
                final Table table = tableOptional.get();
                final Scheme scheme = table.scheme();
                final List<String> schemeFieldsNames = scheme.fields().stream()
                        .map(Field::name)
                        .collect(Collectors.toList());

                final Set<Field> fields = new HashSet<>();
                final List<String> inputFieldsNames = Arrays.stream(sql)
                        .filter(schemeFieldsNames::contains)
                        .collect(Collectors.toList());

                final List<Field> finalFields = scheme.fields()
                        .stream()
                        .filter(it -> inputFieldsNames.contains(it.name()))
                        .collect(Collectors.toList());

                final List<byte[]> values = new ArrayList<>();

                boolean is = false;
                for (String s : sql) {
                    if (is) {
                        if (s.matches("^[a-zA-Z0-9_]*$")) {
                            values.add(s.getBytes());
                        }
                    }
                    if ("values".equals(s)) {
                        is = true;
                    }
                }
                for (int i = 0; i < finalFields.size(); ++i) {
                    fields.add(new FieldImpl(
                            finalFields.get(i).name(),
                            finalFields.get(i).type(),
                            finalFields.get(i).nullable(),
                            finalFields.get(i).unique(),
                            values.get(i)
                    ));
                }

                final DataManipulationQuery dataManipulationQuery = new DataManipulationQuery(
                        userContext.databaseName(),
                        tableName,
                        DataManipulationQuery.Type.INSERT,
                        fields
                );

                return databaseManager.resolveDataManipulationQuery(dataManipulationQuery).toBytes();
            }
        }
        return error();
    }

    private byte[] selectFromTable(@NotNull final UserContext userContext, @NotNull final String[] sql) {
        final Optional<Database> databaseOptional = databaseContext.findByName(userContext.databaseName());
        final String tableName = sql[3];

        if (databaseOptional.isPresent()) {
            final Optional<Table> tableOptional = databaseOptional.get().findTableByName(tableName);
            if (tableOptional.isPresent()) {
                final Table table = tableOptional.get();
                final Scheme scheme = table.scheme();
                table.search();
                return error();
            }
        }
        return error();
    }

    private byte[] error() {
        return new ErrorResultQuery().toBytes();
    }
}