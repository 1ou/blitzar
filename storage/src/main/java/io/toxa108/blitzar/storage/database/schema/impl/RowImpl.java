package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.io.Byteble;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

import java.util.Set;
import java.util.stream.Collectors;

public class RowImpl implements Row, Byteble {
    private final Key key;
    private final Set<Field> fields;

    public RowImpl(@NotNull final Key key, @NotNull final Set<Field> fields) {
        this.fields = fields;
        this.key = key;
    }

    @Override
    public Set<Field> fields() {
        return fields;
    }

    @Override
    public Field fieldByName(@NotNull final String name) {
        return fields.stream()
                .filter(it -> it.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    @Override
    public Key key() {
        return key;
    }

    @Override
    public Set<Field> dataFields() {
        return fields.stream()
                .filter(it -> !it.name().equals(key.field().name()))
                .collect(Collectors.toSet());
    }

    @Override
    public byte[] toBytes() {
        return fields.stream()
                .map(it -> {
                    String v;
                    switch (it.type()) {
                        case SHORT:
                            v = String.valueOf(BytesManipulator.bytesToShort(it.value()));
                            break;
                        case INTEGER:
                            v = String.valueOf(BytesManipulator.bytesToInt(it.value()));
                            break;
                        case LONG:
                            v = String.valueOf(BytesManipulator.bytesToLong(it.value()));
                            break;
                        case VARCHAR:
                            v = new String(it.value());
                            break;
                        default:
                            v = "";
                    }
                    return String.format("%s %s", it.name(), v);
                })
                .collect(Collectors.joining(" | "))
                .getBytes();
    }
}
