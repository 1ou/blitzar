package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.Field;

import java.util.Set;

public class DataManipulationQuery extends AbstractQuery {
    public enum Type {
        /**
         * Insert
         */
        INSERT,
        /**
         * Update
         */
        UPDATE,
        /**
         * Delete
         */
        DELETE,
        /**
         * Select
         */
        SELECT
    }

    ;
    private final Type type;
    private final Set<Field> fields;

    public DataManipulationQuery(@NotNull final String databaseName,
                                 @NotNull final String tableName,
                                 @NotNull final Type type,
                                 @NotNull final Set<Field> fields) {
        super(databaseName, tableName);
        this.type = type;
        this.fields = fields;
    }

    public Type type() {
        return type;
    }

    public Set<Field> fields() {
        return fields;
    }
}
