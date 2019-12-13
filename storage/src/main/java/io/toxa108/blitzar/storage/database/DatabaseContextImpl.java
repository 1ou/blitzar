package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class DatabaseContextImpl implements DatabaseContext {
    private List<Database> databases;

    public DatabaseContextImpl() {
        databases = new ArrayList<>();
    }

    @Override
    public Database findByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("Database isn't specified");
        }

        return databases.stream()
                .filter(it -> it.name().equals(name))
                .findAny()
                .orElseThrow(() -> new NoSuchElementException(
                                String.format("Database %s is not found", name)
                        )
                );
    }
}
