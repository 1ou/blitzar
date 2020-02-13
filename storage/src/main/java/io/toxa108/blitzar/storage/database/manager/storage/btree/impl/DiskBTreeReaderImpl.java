package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskBTreeReader;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableBTreeMetadata;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.impl.BzField;
import io.toxa108.blitzar.storage.database.schema.impl.BzKey;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIo;

import java.io.File;
import java.io.IOException;

public class DiskBTreeReaderImpl implements DiskBTreeReader {
    /**
     * Disk writer
     */
    private final DiskReader diskReader;

    /**
     * Table metadata
     */
    private final TableBTreeMetadata tableBTreeMetadata;

    private final int pNonLeaf;
    private final int pLeaf;

    public DiskBTreeReaderImpl(final File file,
                               final TableBTreeMetadata tableBTreeMetadata) throws IOException {
        this.diskReader = new DiskReaderIo(file);
        this.tableBTreeMetadata = tableBTreeMetadata;
        this.pLeaf = tableBTreeMetadata.entriesInLeafNodeNumber();
        this.pNonLeaf = tableBTreeMetadata.entriesInNonLeafNodeNumber();
    }


    private void checkNodeSize(final int size, final int availableSize) {
        if (availableSize < size) {
            throw new IllegalArgumentException("Node size can't be longer than size of disk page");
        }
    }

    @Override
    public TreeNode read(final int pos) throws IOException {
        Field primaryIndexField = tableBTreeMetadata.primaryIndexField();

        byte[] bytes = diskReader.read(pos, tableBTreeMetadata.databaseConfiguration().diskPageSize());
        boolean isLeaf = bytes[0] == 1;
        byte[] amountOfEntriesBytes = new byte[Integer.BYTES];
        System.arraycopy(bytes, Byte.BYTES, amountOfEntriesBytes, 0, Integer.BYTES);
        int amountOfEntries = BytesManipulator.bytesToInt(amountOfEntriesBytes);

        if (amountOfEntries == 0) {
            return new TreeNode(pos, new Key[pLeaf], new byte[pLeaf][tableBTreeMetadata.dataSize()], true, 0, -1);
        }

        if (!isLeaf) {
            Key[] keys = new Key[this.pNonLeaf];
            int[] p = new int[this.pNonLeaf + 1];

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, tableBTreeMetadata.reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

                int posOfIndex = BytesManipulator.bytesToInt(entryPosBytes) - pos;
                System.arraycopy(bytes, posOfIndex, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentIndex = BytesManipulator.bytesToInt(tmpByteBuffer);

                System.arraycopy(bytes, posOfIndex + Integer.BYTES, tmpByteBuffer, 0, Integer.BYTES);
                p[i] = BytesManipulator.bytesToInt(tmpByteBuffer);
                byte[] currentIndexBytes = new byte[sizeOfCurrentIndex];
                System.arraycopy(bytes, posOfIndex + Integer.BYTES * 2, currentIndexBytes, 0, sizeOfCurrentIndex);
                keys[i] = new BzKey(new BzField(
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

            byte[][] values = new byte[this.pLeaf][tableBTreeMetadata.dataSize()];
            int next = -1;

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex, sizeOfCurrentData;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, tableBTreeMetadata.reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

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

                keys[i] = new BzKey(new BzField(
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
