package io.toxa108.blitzar.storage.io;

import java.io.IOException;

public interface DiskWriter {
    /**
     * Write bytes array from pos position
     *
     * @param pos   position
     * @param bytes bytes
     * @throws IOException disk io exception
     */
    void write(int pos, byte[] bytes) throws IOException;
}
