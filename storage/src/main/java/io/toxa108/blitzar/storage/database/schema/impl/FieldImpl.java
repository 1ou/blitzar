package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;

/**
 * @author toxa
 */
public class FieldImpl implements Field {
    private final String name;
    private final FieldType fieldType;
    private Object value;

    public FieldImpl(String name, FieldType fieldType) {
        this.name = name;
        this.fieldType = fieldType;
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
        return new byte[0];
    }
}
