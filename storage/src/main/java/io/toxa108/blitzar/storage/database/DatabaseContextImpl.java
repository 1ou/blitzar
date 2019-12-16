package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.impl.DatabaseImpl;
import io.toxa108.blitzar.storage.io.FileManager;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DatabaseContextImpl implements DatabaseContext {
    private final List<Database> databases;
    private final FileManager fileManager;

    public DatabaseContextImpl(FileManager fileManager) {
        this.fileManager = fileManager;

        this.databases = fileManager.databases()
                .stream()
                .map(it -> new DatabaseImpl(it, fileManager))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Database> findByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("Database isn't specified");
        }

        return databases.stream()
                .filter(it -> it.name().equals(name))
                .findAny();
    }

    @Override
    public Database createDatabase(String name) {
        Database database = fileManager.initializeDatabase(name);
        databases.add(database);
        return database;
    }

    @Override
    public List<Database> databases() {
        return databases;
    }
}
