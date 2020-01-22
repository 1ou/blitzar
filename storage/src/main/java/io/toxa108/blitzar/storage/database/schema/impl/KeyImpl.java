package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

import java.util.Objects;

public class KeyImpl implements Key {
    private final Field field;

    public KeyImpl(@NotNull final Field field) {
        this.field = field;
    }

    @Override
    public int compareTo(@NotNull final Key key) {
        switch (key.field().type()) {
            case SHORT:
                return Short.compare(BytesManipulator.bytesToShort(field.value()),
                        BytesManipulator.bytesToShort(key.field().value()));
            case INTEGER:
                return Integer.compare(BytesManipulator.bytesToInt(field.value()),
                        BytesManipulator.bytesToInt(key.field().value()));
            case LONG:
                return Long.compare(BytesManipulator.bytesToLong(field.value()),
                        BytesManipulator.bytesToLong(key.field().value()));
            case VARCHAR:
                return new String(field.value()).compareTo(new String(key.field().value()));
            default:
                throw new IllegalArgumentException(
                        String.format("Can't handle key with type %s", key.field().type().name()));
        }
    }

    @Override
    public Field field() {
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyImpl key = (KeyImpl) o;
        return Objects.equals(field, key.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
