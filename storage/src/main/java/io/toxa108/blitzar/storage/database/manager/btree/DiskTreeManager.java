package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.TableDataManager;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIoImpl;
import io.toxa108.blitzar.storage.io.impl.DiskWriterIoImpl;

import java.io.File;
import java.io.IOException;

/**
 * Threadsafe b-plus-tree on disk realization
 */
public class DiskTreeManager implements TableDataManager {
    private final transient DiskReader diskReader;
    private final transient DiskWriter diskWriter;
    private final transient DatabaseConfiguration databaseConfiguration;
    private final transient BytesManipulator bytesManipulator;
    private final transient Scheme scheme;

    public DiskTreeManager(final File file,
                           final DatabaseConfiguration databaseConfiguration,
                           final Scheme scheme) {
        try {
            this.diskReader = new DiskReaderIoImpl(file);
            this.diskWriter = new DiskWriterIoImpl(file);
            this.databaseConfiguration = databaseConfiguration;
            this.bytesManipulator = new BytesManipulatorImpl();
            this.scheme = scheme;
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    public static class TreeNode {

        TreeNode(int q) {
            this.keys = new Key[q];
            this.p = new int[q + 1];
            this.leaf = true;
            this.q = 0;
            this.next = -1;
        }

        TreeNode(Key[] keys, int[] p, boolean isLeaf, int q, int next) {
            this.keys = keys;
            this.p = p;
            this.leaf = isLeaf;
            this.q = q;
            this.next = next;
        }

        Key[] keys;
        int[] p;
        byte[] value;
        boolean leaf;
        int q;
        int next;
    }

    @Override
    public void addRow(final Row row) throws IOException {

        byte[] bytes = diskReader.read(databaseConfiguration.metadataSize() + 1, Byte.BYTES);
        /*
            If b-tree is empty
        */
        if (bytes[0] == 0) {

        } else {

        }
    }

    TreeNode loadNode(final int pos) throws IOException {
        byte[] bytes = diskReader.read(pos, databaseConfiguration.diskPageSize());
        boolean isLeaf = bytes[0] == 1;
        byte[] amountOfEntriesBytes = new byte[Integer.BYTES];
        System.arraycopy(bytes, Byte.BYTES, amountOfEntriesBytes, 0, Integer.BYTES);
        int amountOfEntries = bytesManipulator.bytesToInt(amountOfEntriesBytes);

        int[] keys = new int[amountOfEntries];
        int[] p = new int[amountOfEntries + 1];

        byte[] entrySeekBytes = new byte[Integer.BYTES];
        int entrySeek;
        byte[] indexSizeBytes = new byte[Integer.BYTES];
        byte[] dataSizeBytes = new byte[Integer.BYTES];
        int indexSize, dataSize;

        for (int i = 0; i < amountOfEntries; ++i) {
            System.arraycopy(bytes, Byte.BYTES + Integer.BYTES + Integer.BYTES * i, entrySeekBytes, 0, Integer.BYTES);
            entrySeek = bytesManipulator.bytesToInt(entrySeekBytes);

            System.arraycopy(bytes,
                    entrySeek,
                    indexSizeBytes,
                    0,
                    Integer.BYTES
            );
            indexSize = bytesManipulator.bytesToInt(indexSizeBytes);
            byte[] indexValueBytes = new byte[indexSize];

            if (isLeaf) {
                System.arraycopy(bytes,
                        entrySeek + Integer.BYTES + Integer.BYTES,
                        indexValueBytes,
                        0,
                        indexSize
                );

                System.arraycopy(bytes,
                        entrySeek + Integer.BYTES,
                        dataSizeBytes,
                        0,
                        Integer.BYTES
                );

                dataSize = bytesManipulator.bytesToInt(dataSizeBytes);
                byte[] dataValueBytes = new byte[dataSize];

                System.arraycopy(bytes,
                        entrySeek + indexSize + Integer.BYTES,
                        dataValueBytes,
                        0,
                        dataSize
                );
            } else {
                System.arraycopy(bytes,
                        entrySeek + Integer.BYTES,
                        indexValueBytes,
                        0,
                        indexSize
                );
            }
        }
        return null;
    }

    void saveNode(final int pos, final TreeNode node) throws IOException {
        if (!node.leaf) {
            int estimatedSize = estimateSizeOfElementsInLeafNode(scheme);

            int currPos = pos;
            diskWriter.write(currPos, new byte[]{0});
            currPos++;
            diskWriter.write(currPos, bytesManipulator.intToBytes(node.q));
            int seek = 0;
            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                int posOfIndex = pos + reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * primaryIndexNonVariableRecordSize();

                diskWriter.write(currPos, bytesManipulator.intToBytes(posOfIndex));
                diskWriter.write(posOfIndex, bytesManipulator.intToBytes(scheme.primaryIndexSize()));
                diskWriter.write(posOfIndex + Integer.BYTES, bytesManipulator.intToBytes(node.p[i]));
                byte[] indexValue = node.keys[i].field().value();
                diskWriter.write(posOfIndex + 2 * Integer.BYTES, indexValue);

                if (i == node.q - 1) {
                    diskWriter.write(
                            posOfIndex + 2 * Integer.BYTES + indexValue.length,
                            bytesManipulator.intToBytes(node.p[node.q + 1]));
                }
            }
        }
    }

    /**
     * Works only for not variable rows
     * Return the amount of elements can be stored in the node
     *
     * @param scheme scheme
     * @return number of elements
     */
    int estimateSizeOfElementsInLeafNode(Scheme scheme) {
        int reservedSpace = reservedSpaceInNode();
        int nodeSize = databaseConfiguration.diskPageSize();
        int recordSize = scheme.recordSize()
                + Integer.BYTES  // seek pointer size
                + Integer.BYTES // size of index
                + Integer.BYTES; // size of data

        double bfr = ((double) nodeSize - reservedSpace) / recordSize;
        return (int) bfr;
    }

    int primaryIndexNonVariableRecordSize() {
        return scheme.primaryIndexSize()
                + Integer.BYTES  // seek pointer size
                + Integer.BYTES; // size of index
    }

    /**
     * Works only for not variable rows
     * Return the amount of elements can be stored in the node
     *
     * @param scheme scheme
     * @return number of elements
     */
    int estimateSizeOfElementsInNonLeafNode(Scheme scheme) {
        int reservedSpace = reservedSpaceInNode()
                + Integer.BYTES; // additional N child pointer
        int nodeSize = databaseConfiguration.diskPageSize();
        int recordSize = primaryIndexNonVariableRecordSize();

        double bfr = ((double) nodeSize - reservedSpace) / recordSize;
        return (int) bfr;
    }

    int reservedSpaceInNode() {
        return Byte.BYTES + Integer.BYTES;
    }

    /**
     * Check if the size of entry will be variable or not
     *
     * @return node variability
     */
    private boolean isNodeVariable() {
        return scheme.isVariable();
    }
}
