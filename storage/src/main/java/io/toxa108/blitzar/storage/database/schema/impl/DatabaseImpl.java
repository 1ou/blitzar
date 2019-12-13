package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final List<Table> tables;
    private final String name;
    private final String nameRegex = "[a-zA-Z]+";

    public DatabaseImpl(String name, List<Table> tables) {
        this.tables = tables;
        this.name = name;
    }

    public DatabaseImpl(String name) {
        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect database name");
        }

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

    @Override
    public ResultQuery initializeDatabase() {
        File newDirectory = new File(new File(System.getProperty("java.io.tmpdir")), name);
        if (newDirectory.mkdir()) {
            return new EmptySuccessResultQuery();
        } else {
            return new ErrorResultQuery();
        }
    }
}
