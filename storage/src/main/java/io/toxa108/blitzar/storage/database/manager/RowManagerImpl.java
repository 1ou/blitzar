package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.btree.impl.DiskTreeManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RowManagerImpl implements RowManager {
    private final DiskTreeManager diskTreeManager;

    public RowManagerImpl(final File file,
                          final Scheme scheme,
                          final DatabaseConfiguration databaseConfiguration) {
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

    @Override
    public List<Row> search(Field field) {
        try {
            return diskTreeManager.search(field);
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
