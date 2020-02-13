package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskPage;

public class BzDiskPage implements DiskPage {
    /**
     * The size in kilobytes
     * By default equals 16 kilobytes
     */
    private final int size;

    public BzDiskPage() {
        this.size = 16;
    }

    public BzDiskPage(final int size) {
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }
}
