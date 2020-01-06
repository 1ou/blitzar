package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;

import java.util.Objects;

public class FieldImpl implements Field {
    private final String name;
    private final FieldType fieldType;
    private final Nullable nullable;
    private final Unique unique;
    private final byte[] value;
    private final int valueSize;
    private final BytesManipulator bytesManipulator = new BytesManipulatorImpl();

    public FieldImpl(@NotNull final String name,
                     @NotNull final FieldType fieldType,
                     @NotNull final Nullable nullable,
                     @NotNull final Unique unique,
                     @NotNull final byte[] value) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = nullable;
        this.unique = unique;

        if (fieldType == FieldType.VARCHAR) {
            this.valueSize = value.length;
        } else {
            this.valueSize = fieldType.size();
        }
        this.value = value;
    }

    public FieldImpl(@NotNull final byte[] bytes) {
        this.valueSize = 0;
        value = new byte[0];

        byte[] sizeBytes = new byte[Integer.BYTES];
        byte[] fieldTypeBytes = new byte[Short.BYTES];
        byte[] nullableBytes = new byte[Short.BYTES];
        byte[] uniqueBytes = new byte[Short.BYTES];

        System.arraycopy(bytes, 0, sizeBytes, 0, Integer.BYTES);
        System.arraycopy(bytes, Integer.BYTES, fieldTypeBytes, 0, Short.BYTES);
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES, nullableBytes, 0, Short.BYTES);
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES + Short.BYTES, uniqueBytes, 0, Short.BYTES);

        int size = bytesManipulator.bytesToInt(sizeBytes);
        fieldType = FieldType.fromId(bytesManipulator.bytesToShort(fieldTypeBytes));
        nullable = Nullable.fromId(bytesManipulator.bytesToShort(nullableBytes));
        unique = Unique.fromId(bytesManipulator.bytesToShort(uniqueBytes));

        int fieldNameSize = size - Short.BYTES - Short.BYTES - Short.BYTES;
        byte[] fieldNameBytes = new byte[fieldNameSize];
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES + Short.BYTES + Short.BYTES, fieldNameBytes, 0, fieldNameSize);

        name = new String(fieldNameBytes).replaceAll("%", "");
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
        int size = name.length() + 1
                + Short.BYTES
                + Short.BYTES
                + Short.BYTES;

        byte[] sizeBytes = bytesManipulator.intToBytes(size);
        byte[] fieldTypeBytes = bytesManipulator.shortToBytes(fieldType.id());
        byte[] nullableBytes = bytesManipulator.shortToBytes(nullable.id());
        byte[] uniqueBytes = bytesManipulator.shortToBytes(unique.id());
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
        FieldImpl field = (FieldImpl) o;
        return Objects.equals(name, field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
