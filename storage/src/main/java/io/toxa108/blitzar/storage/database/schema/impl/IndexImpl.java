package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;

import java.util.List;

public class IndexImpl implements Index {
    private final List<Field> fields;
    private final IndexType indexType;

    public IndexImpl(List<Field> fields, IndexType indexType) {
        this.fields = fields;
        this.indexType = indexType;
    }
}
