package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;

import java.util.Objects;

public class FieldImpl implements Field {
    private final String name;
    private final FieldType fieldType;
    private final Nullable nullable;
    private final Unique unique;
    private final byte[] value;
    private final int size;

    public FieldImpl(String name, FieldType fieldType) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = Nullable.NOT_NULL;
        this.unique = Unique.UNIQUE;

        switch (fieldType) {
            case SHORT:
                this.size = Short.BYTES;
                break;
            case INTEGER:
                this.size = Integer.BYTES;
                break;
            case LONG:
                this.size = Long.BYTES;
                break;
            default:
                throw new IllegalStateException("Size of value isn't specified");
        }
        this.value = new byte[size];
    }

    public FieldImpl(String name, FieldType fieldType, int size) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = Nullable.NOT_NULL;
        this.unique = Unique.UNIQUE;

        switch (fieldType) {
            case SHORT:
                this.size = Short.BYTES;
                break;
            case INTEGER:
                this.size = Integer.BYTES;
                break;
            case LONG:
                this.size = Long.BYTES;
                break;
            default:
                this.size = size;
        }
        this.value = new byte[size];
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldImpl field = (FieldImpl) o;
        return Objects.equals(name, field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
