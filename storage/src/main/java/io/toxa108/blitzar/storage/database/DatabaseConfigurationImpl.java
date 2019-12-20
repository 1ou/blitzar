package io.toxa108.blitzar.storage.database;

public class DatabaseConfigurationImpl implements DatabaseConfiguration {
    private final int metadataSize;
    private final int diskPageSize;

    public DatabaseConfigurationImpl(int diskPageSize) {
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
