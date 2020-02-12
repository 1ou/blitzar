package io.toxa108.blitzar.storage.database.schema.transform.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.transform.ToString;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

/**
 * Transform filed to the human readable string representation
 */
public class FieldValueAsString implements ToString {
    private final Field field;

    public FieldValueAsString(final Field field) {
        this.field = field;
    }

    /**
     * Transform filed value to string representation
     * @return string value
     */
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
