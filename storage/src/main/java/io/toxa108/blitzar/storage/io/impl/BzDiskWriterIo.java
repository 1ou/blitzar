package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskWriter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Disk writer shouldn't know about all high level logic.
 * It works only with bytes and files.
 */
public class BzDiskWriterIo implements DiskWriter {
    private final FileChannel fileChannel;

    public BzDiskWriterIo(final File file) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public void write(final int pos, final byte[] bytes) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        fileChannel.write(byteBuffer, pos);
    }
}
