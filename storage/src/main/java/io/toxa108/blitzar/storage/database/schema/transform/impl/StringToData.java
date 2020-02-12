package io.toxa108.blitzar.storage.database.schema.transform.impl;

import io.toxa108.blitzar.storage.database.schema.impl.FieldType;
import io.toxa108.blitzar.storage.database.schema.transform.ToBytes;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

/**
 * Transform value from SQL query to the byte representation
 */
public class StringToData implements ToBytes {
    private final String data;
    private final FieldType fieldType;

    /**
     * Ctor.
     *
     * @param data      data
     * @param fieldType type of field
     */
    public StringToData(final String data,
                        final FieldType fieldType) {
        this.data = data;
        this.fieldType = fieldType;
    }

    @Override
    public byte[] transform() {
        byte[] value;
        switch (fieldType) {
            case LONG:
                value = BytesManipulator.longToBytes(Long.parseLong(data));
                break;
            case INTEGER:
                value = BytesManipulator.intToBytes(Integer.parseInt(data));
                break;
            case SHORT:
                value = BytesManipulator.shortToBytes(Short.parseShort(data));
                break;
            case VARCHAR:
                value = data.getBytes();
                break;
            default:
                value = new byte[0];
        }
        return value;
    }
}
