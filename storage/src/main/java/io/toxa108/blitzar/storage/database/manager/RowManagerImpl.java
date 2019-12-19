package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.schema.Row;

import java.io.File;

public class RowManagerImpl implements RowManager {
    private final File file;

    public RowManagerImpl(File file) {
        this.file = file;
    }

    @Override
    public void add(final Row row) {

    }
}
