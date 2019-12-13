package io.toxa108.blitzar.storage.query;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;

public interface QueryContext {
    Database database();
    Table table();
}
