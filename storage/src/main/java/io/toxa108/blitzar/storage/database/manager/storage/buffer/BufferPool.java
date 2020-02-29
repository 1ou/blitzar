package io.toxa108.blitzar.storage.database.manager.storage.buffer;

import java.io.IOException;

public interface BufferPool {
    /**
     * Load from pool
     *
     * @param pos pom
     * @return bytes
     */
    byte[] fetch(int pos);

    /**
     * Load from disk
     *
     * @param pos pos
     * @return bytes
     */
    byte[] read(int pos) throws IOException;

    /**
     * Push to pool
     *
     * @param pos  pos
     * @param data data
     */
    void pin(int pos, byte[] data);

    /**
     * Write to disk and pool
     *
     * @param pos  pos
     * @param data data
     */
    void write(int pos, byte[] data) throws IOException;
}
