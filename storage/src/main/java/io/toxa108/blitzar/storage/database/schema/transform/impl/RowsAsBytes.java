package io.toxa108.blitzar.storage.database.schema.transform.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.transform.ToString;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transform rows to string representation
 */
public class RowsAsBytes implements ToString {
    /**
     * Rows
     */
    final List<Row> rows;

    /**
     * Mapper to string format
     */
    final private Function<? super Field, ? extends String> map = (field) -> {
        String data = new FieldValueAsString(field).transform();
        return String.format("%s %s", field.name(), data);
    };

    public RowsAsBytes(final List<Row> rows) {
        this.rows = rows;
    }

    /**
     * Rows to bytes transformation.
     * @return bytes
     */
    @Override
    public String transform() {
        return rows.stream()
                .map(it -> it.fields()
                        .stream()
                        .map(map)
                        .collect(Collectors.joining(" | "))
                        + "\n"
                )
                .reduce((l, r) -> l + r)
                .orElse("");
    }

}
