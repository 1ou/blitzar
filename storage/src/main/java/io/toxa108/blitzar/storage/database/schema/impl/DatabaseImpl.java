package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.List;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final List<Table> tables;

    public DatabaseImpl(List<Table> tables) {
        this.tables = tables;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public Optional<Table> findTableByName(String name) {
        return tables.stream()
                .filter(it -> it.name().equalsIgnoreCase(name))
                .findAny();
    }

    @Override
    public Table createTable() {
        return new TableImpl();
    }
}
