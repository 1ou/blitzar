package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.manager.row.RowManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.List;
import java.util.Objects;

/**
 * Table doesn't know anything about Database.
 * But knows about below layers (fields, indexes)
 */
public class BzTable implements Table {
    private final String name;
    private final Scheme scheme;
    private final RowManager rowManager;
    private final State state;

    public BzTable(final String name,
                   final Scheme scheme,
                   final RowManager rowManager) {
        this.rowManager = rowManager;
        this.name = name;
        this.state = State.EXISTS;
        this.scheme = scheme;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public Scheme scheme() {
        return scheme;
    }

    @Override
    public void addRow(final Row row) {
        rowManager.add(row);
    }

    @Override
    public List<Row> search() {
        return rowManager.search();
    }

    @Override
    public List<Row> search(Field field) {
        return rowManager.search(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BzTable table = (BzTable) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
