package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.schema.Field;

import java.util.ArrayList;
import java.util.List;

public class DataDefinitionQuery extends AbstractQuery {
    public DataDefinitionQuery(String database,
                               Type type) {
        super(database, "");
        this.type = type;
        this.fields = new ArrayList<>();
    }

    public DataDefinitionQuery(String database,
                               String table,
                               List<Field> fields,
                               Type type) {
        super(database, table);
        this.type = type;
        this.fields = fields;
    }

    /**
     * Type of request
     */
    public enum Type {
        /**
         * Create database
         */
        CREATE_DATABASE,
        /**
         * Drop database
         */
        DROP_DATABASE,
        /**
         * Create index
         */
        CREATE_INDEX,
        /**
         * Drop index
         */
        DROP_INDEX,
        /**
         * Create table
         */
        CREATE_TABLE,
        /**
         * Drop table
         */
        DROP_TABLE
    };
    private final Type type;

    public Type type() {
        return type;
    }

    private final List<Field> fields;

    public List<Field> fields() {
        return fields;
    }
}
