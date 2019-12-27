package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;

import java.util.Objects;

public class KeyImpl implements Key {
    private final Field field;
    private final BytesManipulator bytesManipulator = new BytesManipulatorImpl();

    public KeyImpl(@NotNull final Field field) {
        this.field = field;
    }

    @Override
    public int compareTo(@NotNull final Key key) {
        switch (key.field().type()) {
            case SHORT:
                return Short.compare(bytesManipulator.bytesToShort(field.value()),
                        bytesManipulator.bytesToShort(key.field().value()));
            case INTEGER:
                return Integer.compare(bytesManipulator.bytesToInt(field.value()),
                        bytesManipulator.bytesToInt(key.field().value()));
            case LONG:
                return Long.compare(bytesManipulator.bytesToLong(field.value()),
                        bytesManipulator.bytesToLong(key.field().value()));
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
