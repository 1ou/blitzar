package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;

public class FieldImpl implements Field {
    private final String name;
    private final FieldType fieldType;
    private final Nullable nullable;
    private final Unique unique;
    private byte[] value;

    public FieldImpl(String name, FieldType fieldType) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = Nullable.NOT_NULL;
        this.unique = Unique.UNIQUE;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public FieldType type() {
        return fieldType;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public Nullable nullable() {
        return nullable;
    }

    @Override
    public Unique unique() {
        return unique;
    }

    @Override
    public byte[] metadataToBytes() {
        return new byte[0];
    }
}
