package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.btree.DiskTreeManager;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RowManagerImpl implements RowManager {
    private final File file;
    private final Scheme scheme;
    private final DiskTreeManager diskTreeManager;

    public RowManagerImpl(@NotNull final File file,
                          @NotNull final Scheme scheme,
                          @NotNull final DatabaseConfiguration databaseConfiguration) {
        this.file = file;
        this.scheme = scheme;
        this.diskTreeManager = new DiskTreeManager(file, databaseConfiguration, scheme);
    }

    @Override
    public void add(@NotNull final Row row) {
        try {
            diskTreeManager.addRow(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Row> search(Key key) {
        try {
            return diskTreeManager.search(key);
        } catch (IOException e) {
            return null; // todo
        }
    }

    @Override
    public List<Row> search() {
        try {
            return diskTreeManager.search();
        } catch (IOException e) {
            return null; // todo
        }
    }
}
