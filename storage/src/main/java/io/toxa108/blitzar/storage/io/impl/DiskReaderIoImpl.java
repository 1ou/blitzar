package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskReader;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Disk writer shouldn'n know about all high level logic. It works only with bytes and files.
 */
public class DiskReaderIoImpl implements DiskReader {
    private final RandomAccessFile randomAccessFile;
    private final FileInputStream fileInputStream;

    public DiskReaderIoImpl(RandomAccessFile randomAccessFile) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.fileInputStream = new FileInputStream(randomAccessFile.getFD());
    }

    @Override
    public byte[] read(int pos, int len) throws IOException {
        randomAccessFile.seek(pos);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        return bufferedInputStream.readNBytes(len);
    }
}
