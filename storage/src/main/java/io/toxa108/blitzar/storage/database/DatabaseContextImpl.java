package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.impl.DatabaseImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatabaseContextImpl implements DatabaseContext {
    private List<Database> databases;

    public DatabaseContextImpl() {
        databases = new ArrayList<>();
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
        Database database = new DatabaseImpl(name);
        databases.add(database);
        return database;
    }
}
