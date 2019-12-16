package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.FileManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final List<Table> tables;
    private final String name;
    private final FileManager fileManager;

    public DatabaseImpl(String name, List<Table> tables, FileManager fileManager) {
        this.tables = tables;
        this.name = name;
        this.fileManager = fileManager;
    }

    public DatabaseImpl(String name, FileManager fileManager) {
        if (name == null) {
            throw new NullPointerException("The table name is not specified");
        }

        this.name = name;
        this.tables = new ArrayList<>();
        this.fileManager = fileManager;
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
        Table table = fileManager.initializeTable(this, name);
        return table;
    }
}
