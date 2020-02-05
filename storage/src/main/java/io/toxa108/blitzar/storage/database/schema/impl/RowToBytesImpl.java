package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.RowToBytes;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Rows abstraction to bytes transformer
 */
public class RowToBytesImpl implements RowToBytes {
    /**
     * Rows
     */
    final List<Row> rows;

    public RowToBytesImpl(final List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public byte[] transform() {
        return rows.stream()
                .map(it -> it.fields()
                        .stream()
                        .map(field -> map.apply(field))
                        .collect(Collectors.joining(" | "))
                        + "\n"
                )
                .reduce((l, r) -> l + r)
                .orElse("")
                .getBytes();
    }

    private Function<? super Field, ? extends String> map = (field) -> {
        String data;
        switch (field.type()) {
            case SHORT:
                data = String.valueOf(BytesManipulator.bytesToShort(field.value()));
                break;
            case INTEGER:
                data = String.valueOf(BytesManipulator.bytesToInt(field.value()));
                break;
            case LONG:
                data = String.valueOf(BytesManipulator.bytesToLong(field.value()));
                break;
            case VARCHAR:
                data = new String(field.value());
                break;
            default:
                data = "";
        }
        return String.format("%s %s", field.name(), data);
    };
}
