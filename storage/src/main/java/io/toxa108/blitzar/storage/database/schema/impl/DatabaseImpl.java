package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;

import java.util.List;

/**
 * @author toxa
 */
public class DatabaseImpl implements Database {
    private final List<Table> tables;

    public DatabaseImpl(List<Table> tables) {
        this.tables = tables;
    }
}
