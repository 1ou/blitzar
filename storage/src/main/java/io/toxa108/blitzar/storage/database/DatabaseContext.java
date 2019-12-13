package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.database.schema.Database;

public interface DatabaseContext {
    Database findByName(String database);
}
