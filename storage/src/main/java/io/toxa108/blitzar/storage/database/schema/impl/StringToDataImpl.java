package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.StringToData;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

public class StringToDataImpl implements StringToData {
    private final String data;
    private final FieldType fieldType;

    public StringToDataImpl(@NotNull final String data,
                            @NotNull final FieldType fieldType) {
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
