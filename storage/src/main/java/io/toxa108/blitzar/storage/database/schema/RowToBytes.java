package io.toxa108.blitzar.storage.database.schema;

public interface RowToBytes {
    /**
     * Transform rows to bytes
     *
     * @return bytes
     */
    byte[] transform();
}
