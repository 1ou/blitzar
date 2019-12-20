package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.btree.DiskTreeManager;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.io.File;
import java.io.IOException;

public class RowManagerImpl implements RowManager {
    private final File file;
    private final Scheme scheme;
    private final DiskTreeManager diskTreeManager;

    public RowManagerImpl(File file, Scheme scheme, DatabaseConfiguration databaseConfiguration) {
        this.file = file;
        this.scheme = scheme;
        this.diskTreeManager = new DiskTreeManager(file, databaseConfiguration, scheme);
    }

    @Override
    public void add(final Row row) {
        try {
            diskTreeManager.addRow(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
