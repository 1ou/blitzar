package io.toxa108.blitzar.storage.io;

import java.io.IOException;

public interface DiskReader {
    /**
     *
     * @param pos position
     * @param len length
     * @return bytes
     * @throws IOException
     */
    byte[] read(int pos, int len) throws IOException;
}
