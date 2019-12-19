package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;

import java.util.Set;

public class RowImpl implements Row {
    private final Key key;
    private final Set<Field> fields;

    public RowImpl(Key key, Set<Field> fields) {
        this.fields = fields;
        this.key = key;
    }

    @Override
    public Set<Field> fields() {
        return fields;
    }

    @Override
    public Key key() {
        return key;
    }
}
