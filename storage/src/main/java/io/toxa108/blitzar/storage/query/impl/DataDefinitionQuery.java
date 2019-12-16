package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;

import java.util.ArrayList;
import java.util.List;

public class DataDefinitionQuery extends AbstractQuery {

    private final Type type;
    private final List<Field> fields;
    private final List<Index> indexes;

    public DataDefinitionQuery(String database,
                               Type type) {
        super(database, "");
        this.type = type;
        this.fields = new ArrayList<>();
        this.indexes = new ArrayList<>();
    }

    public DataDefinitionQuery(String database,
                               String table,
                               List<Field> fields,
                               List<Index> indexes,
                               Type type) {
        super(database, table);
        this.type = type;
        this.fields = fields;
        this.indexes = indexes;
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
    }

    ;

    public Type type() {
        return type;
    }

    public List<Field> fields() {
        return fields;
    }

    public List<Index> getIndexes() {
        return indexes;
    }
}
