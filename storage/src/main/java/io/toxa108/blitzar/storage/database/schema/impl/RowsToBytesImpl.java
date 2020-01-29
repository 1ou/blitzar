package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.RowsToBytes;
import io.toxa108.blitzar.storage.io.Byteble;

import java.util.List;

/**
 * Rows abstraction to bytes transformer
 */
public class RowsToBytesImpl implements RowsToBytes {
    /**
     * Rows
     */
    final List<Row> rows;

    public RowsToBytesImpl(final Row row) {
        this.rows = List.of(row);
    }

    public RowsToBytesImpl(final List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public byte[] transform() {
        return rows.stream()
                .map(it -> {
                    byte[] bytes = ((Byteble) it).toBytes();
                    return new String(bytes) + "\n";
                })
                .reduce((l, r) -> l + r)
                .orElse("")
                .getBytes();
    }
}
