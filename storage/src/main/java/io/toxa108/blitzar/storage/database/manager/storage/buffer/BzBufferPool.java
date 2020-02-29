package io.toxa108.blitzar.storage.database.manager.storage.buffer;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.io.impl.DiskNioReader;
import io.toxa108.blitzar.storage.io.impl.DiskNioWriter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class BzBufferPool implements BufferPool {
    private final ConcurrentHashMap<String, byte[]> buffers;
    private final DiskNioReader diskNioReader;
    private final DiskNioWriter diskNioWriter;
    private final DatabaseConfiguration databaseConfiguration;

    /**
     * Ctor.
     *
     * @param diskNioReader         disk reader
     * @param diskNioWriter         disk writer
     * @param databaseConfiguration database configuration
     */
    public BzBufferPool(final DiskNioReader diskNioReader,
                        final DiskNioWriter diskNioWriter,
                        final DatabaseConfiguration databaseConfiguration) {
        this.diskNioReader = diskNioReader;
        this.diskNioWriter = diskNioWriter;
        this.databaseConfiguration = databaseConfiguration;
        this.buffers = new ConcurrentHashMap<>(1024);
    }

    @Override
    public byte[] fetch(final int pos) {
        return buffers.get(String.valueOf(pos));
    }

    @Override
    public byte[] read(final int pos) throws IOException {
        return diskNioReader.read(pos, databaseConfiguration.diskPageSize());
    }

    @Override
    public void pin(final int pos, final byte[] data) {
        buffers.put(String.valueOf(pos), data);
    }

    @Override
    public void write(final int pos, final byte[] data) throws IOException {
        diskNioWriter.write(pos, data);
    }
}
