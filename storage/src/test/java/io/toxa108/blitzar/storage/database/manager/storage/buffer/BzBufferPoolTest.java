package io.toxa108.blitzar.storage.database.manager.storage.buffer;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.io.impl.DiskNioReader;
import io.toxa108.blitzar.storage.io.impl.DiskNioWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class BzBufferPoolTest {
    BufferPool bufferPool;

    @BeforeEach
    public void init() {
        bufferPool = new BzBufferPool(
                Mockito.mock(DiskNioReader.class),
                Mockito.mock(DiskNioWriter.class),
                Mockito.mock(DatabaseConfiguration.class)
        );
    }

    @Test
    public void pin_Ok() {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        bufferPool.pin(10, bytes);
        assertArrayEquals(bytes, bufferPool.fetch(10));
    }
}