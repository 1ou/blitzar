package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskWriter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Disk writer shouldn't know about all high level logic. It works only with bytes and files.
 */
public class BzDiskWriterIo implements DiskWriter {
    private final File file;
    private final RandomAccessFile randomAccessFile;
//    private final FileOutputStream fileOutputStream;
    private final FileChannel fileChannel;

    public BzDiskWriterIo(final File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
//        this.fileOutputStream = new FileOutputStream(randomAccessFile.getFD());
    }

    @Override
    public void write(final int pos, final byte[] bytes) throws IOException {
//        randomAccessFile.seek(pos);
//        randomAccessFile.write(bytes);

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        fileChannel.write(byteBuffer, pos);
//        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
//        bufferedOutputStream.write(bytes);
    }
}
