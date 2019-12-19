package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.manager.TableDataManager;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIoImpl;
import io.toxa108.blitzar.storage.io.impl.DiskWriterIoImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Threadsafe b-plus-tree on disk realization
 */
public class DiskTreeManager implements TableDataManager {
    private final RandomAccessFile randomAccessFile;
    private final int dataPosition;
    private DiskReader diskReader;
    private final DiskWriter diskWriter;

    public DiskTreeManager(File file, int dataPosition) {
        try {
            RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
            this.randomAccessFile = accessFile;
            this.dataPosition = dataPosition;
            this.diskReader = new DiskReaderIoImpl(accessFile);
            this.diskWriter = new DiskWriterIoImpl(accessFile);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public void addRow(Row row) {
        try {
            this.diskReader = new DiskReaderIoImpl(this.randomAccessFile);
            byte[] bytes = diskReader.read(dataPosition, Byte.BYTES);
            /*
                The b-tree is empty
             */
            if (bytes[0] == 0) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
