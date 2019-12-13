package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final List<Table> tables;
    private final String name;

    public DatabaseImpl(String name, List<Table> tables) {
        this.tables = tables;
        this.name = name;
    }

    public DatabaseImpl(String name) {
        this.name = name;
        this.tables = new ArrayList<>();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Optional<Table> findTableByName(String name) {
        return tables.stream()
                .filter(it -> it.name().equalsIgnoreCase(name))
                .findAny();
    }

    @Override
    public Table createTable(String name) {
        return new TableImpl(name);
    }
}
