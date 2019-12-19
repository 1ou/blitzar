package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskWriter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Disk writer shouldn'n know about all high level logic. It works only with bytes and files.
 */
public class DiskWriterIoImpl implements DiskWriter {
    private final RandomAccessFile randomAccessFile;
//    private final FileOutputStream fileOutputStream;
    private final FileChannel fileChannel;

    public DiskWriterIoImpl(RandomAccessFile randomAccessFile) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = randomAccessFile.getChannel();
//        this.fileOutputStream = new FileOutputStream(randomAccessFile.getFD());
    }

    @Override
    public void write(int pos, byte[] bytes) throws IOException {
//        randomAccessFile.seek(pos);
//        randomAccessFile.write(bytes);

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        fileChannel.write(byteBuffer, pos);
//        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
//        bufferedOutputStream.write(bytes);
    }
}
