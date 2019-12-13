package io.toxa108.blitzar.storage.schema.impl;

import io.toxa108.blitzar.storage.schema.Field;
import io.toxa108.blitzar.storage.schema.Index;

import java.util.List;

public class IndexImpl implements Index {
    private final List<Field> fields;
    private final IndexType indexType;

    public IndexImpl(List<Field> fields, IndexType indexType) {
        this.fields = fields;
        this.indexType = indexType;
    }
}
