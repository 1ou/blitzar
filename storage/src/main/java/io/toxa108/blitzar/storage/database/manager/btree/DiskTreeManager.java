package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.Repository;
import io.toxa108.blitzar.storage.inmemory.entity.Result;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIoImpl;
import io.toxa108.blitzar.storage.io.impl.DiskWriterIoImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Optional;

/**
 * Threadsafe b-plus-tree on disk realization
 *
 * @param <K> index type
 * @param <V> value type
 */
public class DiskTreeManager<K extends Comparable<K>, V> implements Repository<K, V> {
    private final RandomAccessFile randomAccessFile;
    private final int dataPosition;
    private final DiskReader diskReader;
    private final DiskWriter diskWriter;

    public DiskTreeManager(File file, int dataPosition) {
        try (RandomAccessFile accessFile = new RandomAccessFile(file, "rw")) {
            this.randomAccessFile = accessFile;
            this.randomAccessFile.seek(dataPosition);
            this.dataPosition = dataPosition;
            this.diskReader = new DiskReaderIoImpl(accessFile);
            this.diskWriter = new DiskWriterIoImpl(accessFile);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public V add(K key, V value) {
        try {
            byte[] bytes = diskReader.read(dataPosition, Byte.BYTES);
            /*
                The b-tree is empty
             */
            if (bytes[0] == 0) {

            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Result<K, V>> all() {
        return null;
    }

    @Override
    public Optional<V> findByKey(K key) {
        return Optional.empty();
    }

    @Override
    public void removeAll() {

    }
}
