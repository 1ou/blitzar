package io.toxa108.blitzar.storage.database.schema;

public interface RowsToBytes {
    /**
     * Transform rows to bytes
     *
     * @return bytes
     */
    byte[] transform();
}
