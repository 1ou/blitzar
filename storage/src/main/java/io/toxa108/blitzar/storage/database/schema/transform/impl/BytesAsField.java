package io.toxa108.blitzar.storage.database.schema.transform.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.impl.BzField;
import io.toxa108.blitzar.storage.database.schema.impl.FieldType;
import io.toxa108.blitzar.storage.database.schema.impl.Nullable;
import io.toxa108.blitzar.storage.database.schema.impl.Unique;
import io.toxa108.blitzar.storage.database.schema.transform.ToField;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

public class BytesAsField implements ToField {
    private final byte[] bytes;

    public BytesAsField(final byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public Field transform() {
        byte[] value = new byte[0];

        byte[] sizeBytes = new byte[Integer.BYTES];
        byte[] fieldTypeBytes = new byte[Short.BYTES];
        byte[] nullableBytes = new byte[Short.BYTES];
        byte[] uniqueBytes = new byte[Short.BYTES];

        System.arraycopy(bytes, 0, sizeBytes, 0, Integer.BYTES);
        System.arraycopy(bytes, Integer.BYTES, fieldTypeBytes, 0, Short.BYTES);
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES, nullableBytes, 0, Short.BYTES);
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES + Short.BYTES, uniqueBytes, 0, Short.BYTES);

        int size = BytesManipulator.bytesToInt(sizeBytes);
        final FieldType fieldType = FieldType.fromId(BytesManipulator.bytesToShort(fieldTypeBytes));
        final Nullable nullable = Nullable.fromId(BytesManipulator.bytesToShort(nullableBytes));
        final Unique unique = Unique.fromId(BytesManipulator.bytesToShort(uniqueBytes));

        int fieldNameSize = size - Short.BYTES - Short.BYTES - Short.BYTES;
        byte[] fieldNameBytes = new byte[fieldNameSize];
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES + Short.BYTES + Short.BYTES, fieldNameBytes, 0, fieldNameSize);

        final String name = new String(fieldNameBytes).replaceAll("%", "");
        return new BzField(
                name,
                fieldType,
                nullable,
                unique,
                value
        );
    }
}
