package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

import java.util.Objects;

public class BzField implements Field {
    private final String name;
    private final FieldType fieldType;
    private final Nullable nullable;
    private final Unique unique;
    private final byte[] value;
    private final int valueSize;

    public BzField(final String name,
                   final FieldType fieldType,
                   final Nullable nullable,
                   final Unique unique,
                   final byte[] value) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = nullable;
        this.unique = unique;
        this.value = value;

        if (fieldType == FieldType.VARCHAR) {
            this.valueSize = value.length;
        } else {
            this.valueSize = fieldType.size();
        }
    }

    /**
     *
     * @return
     */
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
    public byte[] metadata() {
        int size = name.length() + 1
                + Short.BYTES
                + Short.BYTES
                + Short.BYTES;

        byte[] sizeBytes = BytesManipulator.intToBytes(size);
        byte[] fieldTypeBytes = BytesManipulator.shortToBytes(fieldType.id());
        byte[] nullableBytes = BytesManipulator.shortToBytes(nullable.id());
        byte[] uniqueBytes = BytesManipulator.shortToBytes(unique.id());
        byte[] fieldsBytes = (name + "%").getBytes();

        byte[] resultBytes = new byte[sizeBytes.length + size];
        System.arraycopy(sizeBytes, 0, resultBytes, 0, sizeBytes.length);
        System.arraycopy(fieldTypeBytes, 0, resultBytes, sizeBytes.length, fieldTypeBytes.length);
        System.arraycopy(nullableBytes, 0, resultBytes, sizeBytes.length + fieldTypeBytes.length, nullableBytes.length);
        System.arraycopy(uniqueBytes, 0, resultBytes,
                sizeBytes.length + fieldTypeBytes.length + nullableBytes.length, uniqueBytes.length);
        System.arraycopy(fieldsBytes, 0, resultBytes,
                sizeBytes.length + fieldTypeBytes.length + nullableBytes.length + uniqueBytes.length, fieldsBytes.length);
        return resultBytes;
    }

    @Override
    public int diskSize() {
        return valueSize;
    }

    @Override
    public boolean isVariable() {
        return fieldType.isVariable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BzField field = (BzField) o;
        return Objects.equals(name, field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
