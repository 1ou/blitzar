package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.util.Set;

public class RowImpl implements Row {
    private final Set<Field> fields;

    public RowImpl(Set<Field> fields) {
        this.fields = fields;
    }

    @Override
    public Set<Field> fields() {
        return fields;
    }
}
