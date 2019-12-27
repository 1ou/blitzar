package io.toxa108.blitzar.storage.database;

import io.toxa108.blitzar.storage.NotNull;

public class DatabaseConfigurationImpl implements DatabaseConfiguration {
    /**
     * Metadata size in bytes
     */
    private final int metadataSize;

    /**
     * Disk page size in bytes
     */
    private final int diskPageSize;

    public DatabaseConfigurationImpl(@NotNull final int diskPageSize) {
        this.metadataSize = 1024 * diskPageSize;
        this.diskPageSize = 1024 * diskPageSize;
    }

    @Override
    public int metadataSize() {
        return metadataSize;
    }

    @Override
    public int diskPageSize() {
        return diskPageSize;
    }
}
