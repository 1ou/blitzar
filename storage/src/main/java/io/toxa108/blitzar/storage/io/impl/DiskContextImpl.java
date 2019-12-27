package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.io.DiskContext;
import io.toxa108.blitzar.storage.io.DiskPage;

public class DiskContextImpl implements DiskContext {
    private final DiskPage diskPage;

    public DiskContextImpl(@NotNull final DiskPage diskPage) {
        this.diskPage = diskPage;
    }

    @Override
    public int blockSize() {
        return diskPage.size();
    }
}
