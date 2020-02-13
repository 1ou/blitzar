package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.BzField;
import io.toxa108.blitzar.storage.database.schema.transform.impl.RowsAsBytes;
import io.toxa108.blitzar.storage.database.schema.transform.impl.StringAsFieldValue;
import io.toxa108.blitzar.storage.query.DataManipulationQueryResolver;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.util.Optional;
import java.util.stream.Stream;

import static io.toxa108.blitzar.storage.query.impl.SqlReservedWords.WHERE;

public class SelectFromTableCommand implements SqlCommand {
    private final DatabaseContext databaseContext;
    private final DataManipulationQueryResolver dataManipulationQueryResolver;

    public SelectFromTableCommand(final DatabaseContext databaseContext,
                                  final DataManipulationQueryResolver dataManipulationQueryResolver) {
        this.databaseContext = databaseContext;
        this.dataManipulationQueryResolver = dataManipulationQueryResolver;
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
                    final Field fieldWithValue = new BzField(
                            field.name(),
                            field.type(),
                            field.nullable(),
                            field.unique(),
                            new StringAsFieldValue(extractFieldValue(sql), field.type()).transform()
                    );
                    return new RowsAsBytes(table.search(fieldWithValue)).transform().getBytes();
                } else {
                    return new RowsAsBytes(table.search()).transform().getBytes();
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
