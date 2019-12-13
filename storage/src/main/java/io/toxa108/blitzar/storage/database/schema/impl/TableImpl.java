package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Record;
import io.toxa108.blitzar.storage.database.schema.Table;

public class TableImpl implements Table {
    private final Record record;

    public TableImpl(Record record) {
        this.record = record;
    }
}
