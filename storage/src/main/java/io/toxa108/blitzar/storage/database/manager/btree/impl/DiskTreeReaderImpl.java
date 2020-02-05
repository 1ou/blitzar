package io.toxa108.blitzar.storage.database.manager.btree.impl;

import io.toxa108.blitzar.storage.database.manager.btree.DiskTreeReader;
import io.toxa108.blitzar.storage.database.manager.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.impl.FieldImpl;
import io.toxa108.blitzar.storage.database.schema.impl.KeyImpl;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIoImpl;

import java.io.File;
import java.io.IOException;

public class DiskTreeReaderImpl implements DiskTreeReader {
    /**
     * Disk writer
     */
    private final DiskReader diskReader;

    /**
     * Table metadata
     */
    private final TableTreeMetadata tableTreeMetadata;

    private final int pNonLeaf;
    private final int pLeaf;

    public DiskTreeReaderImpl(final File file,
                              final TableTreeMetadata tableTreeMetadata) throws IOException {
        this.diskReader = new DiskReaderIoImpl(file);
        this.tableTreeMetadata = tableTreeMetadata;
        this.pLeaf = tableTreeMetadata.estimatedSizeOfElementsInLeafNode();
        this.pNonLeaf = tableTreeMetadata.estimatedSizeOfElementsInNonLeafNode();
    }


    private void checkNodeSize(final int size, final int availableSize) {
        if (availableSize < size) {
            throw new IllegalArgumentException("Node size can't be longer than size of disk page");
        }
    }

    @Override
    public TreeNode read(final int pos) throws IOException {
        Field primaryIndexField = tableTreeMetadata.primaryIndexField();

        byte[] bytes = diskReader.read(pos, tableTreeMetadata.databaseConfiguration().diskPageSize());
        boolean isLeaf = bytes[0] == 1;
        byte[] amountOfEntriesBytes = new byte[Integer.BYTES];
        System.arraycopy(bytes, Byte.BYTES, amountOfEntriesBytes, 0, Integer.BYTES);
        int amountOfEntries = BytesManipulator.bytesToInt(amountOfEntriesBytes);

        if (amountOfEntries == 0) {
            return new TreeNode(pos, new Key[pLeaf], new byte[pLeaf][tableTreeMetadata.dataSize()], true, 0, -1);
        }

        if (!isLeaf) {
            Key[] keys = new Key[this.pNonLeaf];
            int[] p = new int[this.pNonLeaf + 1];

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, tableTreeMetadata.reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

                int posOfIndex = BytesManipulator.bytesToInt(entryPosBytes) - pos;
                System.arraycopy(bytes, posOfIndex, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentIndex = BytesManipulator.bytesToInt(tmpByteBuffer);

                System.arraycopy(bytes, posOfIndex + Integer.BYTES, tmpByteBuffer, 0, Integer.BYTES);
                p[i] = BytesManipulator.bytesToInt(tmpByteBuffer);
                byte[] currentIndexBytes = new byte[sizeOfCurrentIndex];
                System.arraycopy(bytes, posOfIndex + Integer.BYTES * 2, currentIndexBytes, 0, sizeOfCurrentIndex);
                keys[i] = new KeyImpl(new FieldImpl(
                        primaryIndexField.name(),
                        primaryIndexField.type(),
                        primaryIndexField.nullable(),
                        primaryIndexField.unique(),
                        currentIndexBytes)
                );

                if (i == amountOfEntries - 1) {
                    int pNextPos = posOfIndex + Integer.BYTES * 2 + currentIndexBytes.length;
                    System.arraycopy(bytes, pNextPos, tmpByteBuffer, 0, Integer.BYTES);
                    p[i + 1] = BytesManipulator.bytesToInt(tmpByteBuffer);
                }
            }
            return new TreeNode(
                    pos,
                    keys,
                    p,
                    false,
                    amountOfEntries,
                    -1
            );
        } else {
            Key[] keys = new Key[this.pLeaf];

            byte[][] values = new byte[this.pLeaf][tableTreeMetadata.dataSize()];
            int next = -1;

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex, sizeOfCurrentData;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, tableTreeMetadata.reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

                int posOfIndex = BytesManipulator.bytesToInt(entryPosBytes) - pos;
                System.arraycopy(bytes, posOfIndex, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentIndex = BytesManipulator.bytesToInt(tmpByteBuffer);

                System.arraycopy(bytes, posOfIndex + Integer.BYTES, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentData = BytesManipulator.bytesToInt(tmpByteBuffer);

                byte[] currentIndexBytes = new byte[sizeOfCurrentIndex];
                byte[] currentDataBytes = new byte[sizeOfCurrentData];
                System.arraycopy(bytes, posOfIndex + 2 * Integer.BYTES, currentIndexBytes, 0, sizeOfCurrentIndex);
                System.arraycopy(bytes, posOfIndex + 2 * Integer.BYTES + sizeOfCurrentIndex,
                        currentDataBytes, 0, sizeOfCurrentData);

                keys[i] = new KeyImpl(new FieldImpl(
                        primaryIndexField.name(),
                        primaryIndexField.type(),
                        primaryIndexField.nullable(),
                        primaryIndexField.unique(),
                        currentIndexBytes)
                );
                values[i] = currentDataBytes;

                if (i == amountOfEntries - 1) {
                    int nextLeafPos = posOfIndex + 2 * Integer.BYTES + sizeOfCurrentIndex + sizeOfCurrentData;
                    System.arraycopy(bytes, nextLeafPos, tmpByteBuffer, 0, Integer.BYTES);
                    next = BytesManipulator.bytesToInt(tmpByteBuffer);
                }
            }
            return new TreeNode(
                    pos,
                    keys,
                    values,
                    true,
                    amountOfEntries,
                    next
            );
        }
    }
}
