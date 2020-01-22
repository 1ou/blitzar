package io.toxa108.blitzar.storage.io;

import java.io.IOException;

public interface DiskReader {
    /**
     * Read N bytes from K byte
     *
     * @param pos position
     * @param len length
     * @return bytes
     * @throws IOException disk io issue
     */
    byte[] read(int pos, int len) throws IOException;
}
