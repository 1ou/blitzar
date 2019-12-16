package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Index;

import java.util.List;

public class IndexImpl implements Index {
    private final List<String> fields;
    private final IndexType type;

    public IndexImpl(List<String> fields, IndexType type) {
        this.fields = fields;
        this.type = type;
    }

    @Override
    public IndexType type() {
        return type;
    }

    @Override
    public List<String> fields() {
        return fields;
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
