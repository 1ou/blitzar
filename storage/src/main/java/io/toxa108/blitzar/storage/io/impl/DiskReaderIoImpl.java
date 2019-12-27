package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.io.DiskReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Disk writer shouldn't know about all high level logic. It works only with bytes and files.
 */
public class DiskReaderIoImpl implements DiskReader {
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final FileInputStream fileInputStream;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer;

    public DiskReaderIoImpl(@NotNull final File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "r");
        this.fileInputStream = new FileInputStream(randomAccessFile.getFD());
        this.fileChannel = randomAccessFile.getChannel();
        this.byteBuffer = ByteBuffer.allocate(1024);
    }

    @Override
    public byte[] read(@NotNull final int pos, @NotNull final int len) throws IOException {
//        randomAccessFile.seek(pos);
//        byte[] res = new byte[len];
//        randomAccessFile.read(res);

        ByteBuffer byteBuffer = ByteBuffer.allocate(len);
        fileChannel.read(byteBuffer, pos);
        return byteBuffer.array();
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
//        return bufferedInputStream.readNBytes(len);
    }
}
