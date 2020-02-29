package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskReader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Disk writer shouldn't know about all high level logic. It works only with bytes and files.
 */
public class DiskNioReader implements DiskReader {
    private final FileChannel fileChannel;

    public DiskNioReader(final File file) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        this.fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public byte[] read(final int pos, final int len) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(len);
        fileChannel.read(byteBuffer, pos);
        return byteBuffer.array();
    }
}
