package io.toxa108.blitzar.storage.schema.impl;

import io.toxa108.blitzar.storage.schema.Field;
import io.toxa108.blitzar.storage.schema.Index;

import java.util.List;

/**
 * @author toxa
 */
public class RecordImpl {
    private final List<Field> fields;
    private final List<Index> indices;

    public RecordImpl(List<Field> fields, List<Index> indices) {
        this.fields = fields;
        this.indices = indices;
    }
}
