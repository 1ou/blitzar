package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskWriter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Disk writer shouldn'n know about all high level logic. It works only with bytes and files.
 */
public class DiskWriterIoImpl implements DiskWriter {
    private final RandomAccessFile randomAccessFile;
    private final FileOutputStream fileOutputStream;

    public DiskWriterIoImpl(RandomAccessFile randomAccessFile) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.fileOutputStream = new FileOutputStream(randomAccessFile.getFD());
    }

    @Override
    public void write(int pos, byte[] bytes) throws IOException {
        randomAccessFile.seek(pos);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        bufferedOutputStream.write(bytes);
    }
}
