package io.toxa108.blitzar.storage.database;

public class DatabaseConfigurationImpl implements DatabaseConfiguration {
    private final int metadataSize;
    private final int diskPageSize;

    public DatabaseConfigurationImpl(int diskPageSize) {
        this.diskPageSize = diskPageSize;
        this.metadataSize = 1024 * diskPageSize;
    }

    @Override
    public int metadataSize() {
        return metadataSize;
    }
}
