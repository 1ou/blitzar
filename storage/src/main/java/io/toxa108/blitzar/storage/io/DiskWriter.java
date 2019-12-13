package io.toxa108.blitzar.storage.io;

import java.io.IOException;

/**
 * @author toxa
 */
public interface DiskWriter {
    void write(int pos, byte[] bytes) throws IOException;
}
