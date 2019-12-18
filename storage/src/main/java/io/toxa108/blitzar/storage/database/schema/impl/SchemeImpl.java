package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.util.Set;

public class SchemeImpl implements Scheme {
    private final Set<Field> fields;
    private final Set<Index> indexes;

    public SchemeImpl(Set<Field> fields, Set<Index> indexes) {
        if (indexes.stream().filter(it -> it.type() == IndexType.PRIMARY).count() > 1) {
            throw new IllegalArgumentException("Table must include only one PRIMARY index");
        }

        this.fields = fields;
        this.indexes = indexes;
    }

    @Override
    public Set<Field> fields() {
        return fields;
    }

    @Override
    public Set<Index> indexes() {
        return indexes;
    }
}
