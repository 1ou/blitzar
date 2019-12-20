package io.toxa108.blitzar.storage.database;

public interface DatabaseConfiguration {
    /**
     * Return metadata size
     *
     * @return size in bytes
     */
    int metadataSize();

    /**
     * Return disk page size in bytes
     *
     * @return disk page size in bytes
     */
    int diskPageSize();
}
