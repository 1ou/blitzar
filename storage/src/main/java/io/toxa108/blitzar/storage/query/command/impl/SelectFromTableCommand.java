package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
import io.toxa108.blitzar.storage.database.manager.DatabaseManager;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.FieldImpl;
import io.toxa108.blitzar.storage.database.schema.impl.RowsToBytesImpl;
import io.toxa108.blitzar.storage.database.schema.impl.StringToDataImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.Optional;
import java.util.stream.Stream;

import static io.toxa108.blitzar.storage.query.impl.SqlReservedWords.WHERE;

public class SelectFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DatabaseManager databaseManager;
    private final ArrayManipulator arrayManipulator;

    public SelectFromTableCommand(final DatabaseContext databaseContext,
                                  final DatabaseManager databaseManager) {
        this.databaseContext = databaseContext;
        this.databaseManager = databaseManager;
        this.arrayManipulator = new ArrayManipulator();
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        final Optional<Database> databaseOptional = databaseContext.findByName(userContext.databaseName());
        final String tableName = sql[3];

        if (databaseOptional.isPresent()) {
            final Optional<Table> tableOptional = databaseOptional.get().findTableByName(tableName);
            if (tableOptional.isPresent()) {
                final Table table = tableOptional.get();

                if (Stream.of(sql).anyMatch(it -> it.equalsIgnoreCase(WHERE.name()))) {
                    final Field field = table.scheme().fieldByName(extractFieldName(sql));
                    final Field fieldWithValue = new FieldImpl(
                            field.name(),
                            field.type(),
                            field.nullable(),
                            field.unique(),
                            new StringToDataImpl(extractFieldValue(sql), field.type()).transform()
                    );
                    return new RowsToBytesImpl(table.search(fieldWithValue)).transform();
                } else {
                    return new RowsToBytesImpl(table.search()).transform();
                }
            }
        }
        return new ErrorResultQuery().toBytes();
    }

    private String extractFieldName(final String[] sql) {
        for (int i = 0; i < sql.length; ++i) {
            if (sql[i].equalsIgnoreCase(WHERE.name()) && i < sql.length - 1) {
                return sql[i + 1];
            }
        }
        throw new IllegalStateException("Incorrect sql");
    }

    private String extractFieldValue(final String[] sql) {
        for (int i = 0; i < sql.length; ++i) {
            if ("=".equals(sql[i]) && i < sql.length - 1) {
                return sql[i + 1];
            }
        }
        throw new IllegalStateException("Incorrect sql");
    }
}
