package io.toxa108.blitzar.storage.query.operation;

import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CreateTableCommand implements SqlCommand {
    private final DatabaseManager databaseManager;

    public CreateTableCommand(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
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
}
