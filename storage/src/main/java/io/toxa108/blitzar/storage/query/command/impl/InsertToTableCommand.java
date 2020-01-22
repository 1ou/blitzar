package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.FieldImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.DataManipulationQuery;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.*;
import java.util.stream.Collectors;

public class InsertToTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;

    public InsertToTableCommand(@NotNull final DatabaseContext databaseContext,
                                @NotNull final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
    }

    @Override
    public byte[] execute(@NotNull final UserContext userContext,
                          @NotNull final String[] sql) {
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
        return new ErrorResultQuery().toBytes();
    }
}