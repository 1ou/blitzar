package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.util.List;

public class SchemeImpl implements Scheme {
    private final List<Field> fields;
    private final List<Index> indexes;

    public SchemeImpl(List<Field> fields, List<Index> indexes) {
        this.fields = fields;
        this.indexes = indexes;
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public List<Index> indexes() {
        return indexes;
    }
}
