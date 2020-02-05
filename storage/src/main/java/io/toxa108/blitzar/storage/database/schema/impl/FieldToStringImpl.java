package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.FieldToString;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

public class FieldToStringImpl implements FieldToString {
    private final Field field;

    public FieldToStringImpl(final Field field) {
        this.field = field;
    }

    @Override
    public String transform() {
        String value;
        switch (field.type()) {
            case LONG:
                value = String.valueOf(BytesManipulator.bytesToLong(field.value()));
                break;
            case INTEGER:
                value = String.valueOf(BytesManipulator.bytesToInt(field.value()));
                break;
            case SHORT:
                value = String.valueOf(BytesManipulator.bytesToShort(field.value()));
                break;
            case VARCHAR:
                value = new String(field.value());
                break;
            default:
                value = "";
        }
        return value;
    }
}
