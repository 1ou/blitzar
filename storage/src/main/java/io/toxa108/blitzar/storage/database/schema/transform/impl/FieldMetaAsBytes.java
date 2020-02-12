package io.toxa108.blitzar.storage.database.schema.transform.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.transform.ToBytes;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

/**
 * Transform field metadata to bytes
 */
public class FieldMetaAsBytes implements ToBytes {
    private final Field field;

    public FieldMetaAsBytes(final Field field) {
        this.field = field;
    }

    @Override
    public byte[] transform() {
        int size = field.name().length() + 1
                + Short.BYTES
                + Short.BYTES
                + Short.BYTES;

        byte[] sizeBytes = BytesManipulator.intToBytes(size);
        byte[] fieldTypeBytes = BytesManipulator.shortToBytes(field.type().id());
        byte[] nullableBytes = BytesManipulator.shortToBytes(field.nullable().id());
        byte[] uniqueBytes = BytesManipulator.shortToBytes(field.unique().id());
        byte[] fieldsBytes = (field.name() + "%").getBytes();

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
}
