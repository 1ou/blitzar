package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.TableDataManager;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.impl.FieldImpl;
import io.toxa108.blitzar.storage.database.schema.impl.KeyImpl;
import io.toxa108.blitzar.storage.database.schema.impl.RowImpl;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIoImpl;
import io.toxa108.blitzar.storage.io.impl.DiskWriterIoImpl;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Threadsafe b-plus-tree on disk realization
 */
public class DiskTreeManager implements TableDataManager {
    private final transient DiskReader diskReader;
    private final transient DiskWriter diskWriter;
    private final transient DatabaseConfiguration databaseConfiguration;
    private final transient BytesManipulator bytesManipulator;
    private final transient Scheme scheme;
    private final transient int pNonLeaf;
    private final transient int pLeaf;
    private int numberOfUsedBlocks;

    public DiskTreeManager(final File file,
                           final DatabaseConfiguration databaseConfiguration,
                           final Scheme scheme) {
        try {
            this.diskReader = new DiskReaderIoImpl(file);
            this.diskWriter = new DiskWriterIoImpl(file);
            this.databaseConfiguration = databaseConfiguration;
            this.bytesManipulator = new BytesManipulatorImpl();
            this.scheme = scheme;
            this.pLeaf = estimateSizeOfElementsInLeafNode(scheme);
            this.pNonLeaf = estimateSizeOfElementsInNonLeafNode(scheme);
            initMetadata();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    private void initMetadata() throws IOException {
        numberOfUsedBlocks = bytesManipulator.bytesToInt(diskReader.read(0, Integer.BYTES));
    }

    private void updateMetadata() throws IOException {
        diskWriter.write(0, bytesManipulator.intToBytes(numberOfUsedBlocks));
    }

    public static class TreeNode {
        Key[] keys;
        int[] p;
        byte[][] values;
        boolean leaf;
        int q;
        int nextPos;
        int pos;

        TreeNode(int q, int dataLen) {
            this.keys = new Key[q];
            this.p = new int[q + 1];
            this.leaf = true;
            this.q = 0;
            this.nextPos = -1;
            this.pos = -1;
            this.values = new byte[q][dataLen];
            for (int i = 0; i < q; i++) {
                values[i] = new byte[dataLen];
            }
        }

        TreeNode(int pos, Key[] keys, int[] p, boolean isLeaf, int q, int nextPos) {
            this.keys = keys;
            this.p = p;
            this.leaf = isLeaf;
            this.q = q;
            this.nextPos = nextPos;
            this.values = new byte[0][0];
            this.pos = pos;
        }

        TreeNode(int pos, Key[] keys, byte[][] values, boolean isLeaf, int q, int nextPos) {
            this.keys = keys;
            this.values = values;
            this.leaf = isLeaf;
            this.q = q;
            this.nextPos = nextPos;
            this.p = new int[q + 1];
            this.pos = pos;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TreeNode treeNode = (TreeNode) o;
            return leaf == treeNode.leaf &&
                    pos == treeNode.pos &&
                    q == treeNode.q &&
                    nextPos == treeNode.nextPos &&
                    Arrays.equals(keys, treeNode.keys) &&
                    Arrays.equals(p, treeNode.p) &&
                    Arrays.deepEquals(values, treeNode.values);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(leaf, q, nextPos);
            result = 31 * result + Arrays.hashCode(keys);
            result = 31 * result + Arrays.hashCode(p);
            result = 31 * result + Arrays.hashCode(values);
            return result;
        }
    }

    @Override
    public void addRow(final Row row) throws IOException {
        TreeNode n = loadNode(databaseConfiguration.metadataSize() + 1);
        Key key = row.key();

        Stack<TreeNode> stack = new Stack<>();

        while (!n.leaf) {
            stack.push(n);
            int q = n.q;
            if (key.compareTo(n.keys[0]) < 0) {
                n = loadNode(n.p[0]);
            } else if (key.compareTo(n.keys[q - 1]) > 0) {
                n = loadNode(n.p[q]);
            } else {
                int fn = search(n.keys, n.q, key);
                n = loadNode(n.p[fn]);
            }
        }

        int properlyPosition = findProperlyPosition(n.keys, n.q, key);
        /*
            If record with such key already exists
         */
        if (properlyPosition == -1) {
            throw new IllegalArgumentException(
                    "key: " + key.field().name() + " was inserted into node [" +
                            Arrays.stream(n.keys).map(it -> it.field().name())
                                    .collect(Collectors.joining(", ")) + "]");
        } else {
            TreeNode newNode = new TreeNode(pLeaf, scheme.dataSize());
            /*
                If leaf is not full, insert new entry in leaf
             */
            if (n.q < this.pLeaf - 1) {
                if (n.q == 0) {
                    numberOfUsedBlocks = 1;
                }

                insertInArray(n.keys, key, properlyPosition);
                insertInArray(n.p, -1, properlyPosition);
                n.q++;
                insertInArray(
                        n.values,
                        rowDataToBytes(row),
                        properlyPosition
                );
                saveNode(n.pos, n);
            }
            /*
                Split leaf before insert
             */
            else {
                int tmpPosition = n.pos;
                TreeNode tmp = new TreeNode(this.pLeaf + 1, scheme.dataSize());
                copyArray(n.keys, tmp.keys, n.q);
                copyArray(n.p, tmp.p, n.q + 1);
                copyArray(n.values, tmp.values, n.q);
                insertInArray(tmp.keys, key, properlyPosition);
                insertInArray(tmp.p, numberOfUsedBlocks, properlyPosition);
                insertInArray(tmp.values, rowDataToBytes(row), properlyPosition);
                tmp.q = n.q + 1;

                newNode.nextPos = n.nextPos;
                int j = (pLeaf + 1) >>> 1;

                n.keys = new Key[pLeaf];
                n.p = new int[pLeaf + 1];
                copyArray(tmp.keys, n.keys, j - 1);
                copyArray(tmp.values, n.values, j - 1);
                copyArray(tmp.p, n.p, j - 1);
                n.q = j - 1;

                copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
                copyArray(tmp.values, newNode.values, j, tmp.q - j);
                copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
                newNode.q = tmp.q - j;
                key = tmp.keys[j];

                int newPosLeft = freeSpacePos();
                int newPosRight = freeSpacePos() + databaseConfiguration.diskPageSize();

                n.nextPos = newPosRight;
                n.pos = newPosLeft;
                newNode.pos = newPosRight;

                saveNode(newPosLeft, n);
                saveNode(newPosRight, newNode);

                numberOfUsedBlocks += 2;

                boolean finished = false;
                while (!finished) {
                    if (stack.isEmpty()) {
                        TreeNode topNode = new TreeNode(pLeaf, scheme.dataSize());
                        topNode.keys[0] = key;
                        topNode.p[0] = n.pos;
                        topNode.p[1] = newNode.pos;
                        topNode.leaf = false;
                        topNode.q = 1;
                        saveNode(tmpPosition, topNode);
                        numberOfUsedBlocks += 1;
                        finished = true;
                    } else {
                        n = stack.pop();
                        /*
                            If internal node n is not full
                            parent node is not full - no split
                         */
                        if (n.q < pNonLeaf - 1) {
                            properlyPosition = findProperlyPosition(n.keys, n.q, key);
                            insertInArray(n.keys, key, properlyPosition);
                            insertInArray(n.p, newNode.pos, properlyPosition + 1);
                            n.q++;
                            saveNode(n.pos, n);
                            finished = true;
                        } else {
                            tmp = new TreeNode(pNonLeaf + 1, scheme.dataSize());
                            copyArray(n.keys, tmp.keys, n.q);
                            copyArray(n.p, tmp.p, n.q + 1);
                            tmp.q = n.q + 1;

                            properlyPosition = findProperlyPosition(n.keys, n.q, key);
                            insertInArray(tmp.keys, key, properlyPosition);
                            insertInArray(tmp.p, newNode.pos, properlyPosition + 1);

                            newNode = new TreeNode(this.pNonLeaf, scheme.dataSize());
                            j = (this.pNonLeaf + 1) >>> 1;

                            n.keys = new Key[pNonLeaf];
                            n.p = new int[pNonLeaf + 1];
                            copyArray(tmp.keys, n.keys, 0, j - 1);
                            copyArray(tmp.p, n.p, 0, j);
                            n.q = j - 1;
                            n.leaf = false;

                            copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
                            copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
                            newNode.q = tmp.q - j;
                            newNode.leaf = false;

                            newPosLeft = freeSpacePos();
                            newPosRight = freeSpacePos() + databaseConfiguration.diskPageSize();

                            tmpPosition = n.pos;
                            n.pos = newPosLeft;
                            newNode.pos = newPosRight;

                            saveNode(newPosLeft, n);
                            saveNode(newPosRight, newNode);
                            numberOfUsedBlocks += 2;

                            key = tmp.keys[j - 1];
                        }
                    }
                }
            }
        }
    }

    @Override
    public Row search(Key key) throws IOException {
        TreeNode n = loadNode(databaseConfiguration.metadataSize() + 1);

        while (!n.leaf) {
            int q = n.q;
            if (key.compareTo(n.keys[0]) < 0) {
                n = loadNode(n.p[0]);
            } else if (key.compareTo(n.keys[q - 1]) > 0) {
                n = loadNode(n.p[q]);
            } else {
                int fn = search(n.keys, n.q, key);
                n = loadNode(n.p[fn]);
            }
        }
        int i = searchKey(n.keys, n.q, key);
        if (i == -1) {
            throw new NoSuchElementException();
        }

        byte[] data = n.values[i];
        AtomicInteger seek = new AtomicInteger();
        Set<Field> fields = scheme.dataFields()
                .stream()
                .map(it -> {
                    int s = seek.getAndAdd(it.diskSize());
                    byte[] bytes = new byte[it.diskSize()];
                    System.arraycopy(data, s, bytes, 0, it.diskSize());
                    return new FieldImpl(
                            it.name(),
                            it.type(),
                            it.nullable(),
                            it.unique(),
                            bytes
                    );
                })
                .collect(Collectors.toSet());
        return new RowImpl(
                key,
                fields
        );
    }

    /**
     * Convert data from row to bytes
     *
     * @param row row
     * @return byte array
     */
    private byte[] rowDataToBytes(Row row) {
        return row.dataFields()
                .stream()
                .map(Field::value)
                .reduce((a, b) -> {
                    byte[] c = new byte[a.length + b.length];
                    System.arraycopy(a, 0, c, 0, a.length);
                    System.arraycopy(b, 0, c, a.length, b.length);
                    return c;
                }).orElse(new byte[0]);
    }

    /**
     * Load node from disk
     *
     * @param pos pos in file
     * @return tree node
     * @throws IOException in case issue with reading
     */
    TreeNode loadNode(final int pos) throws IOException {
        Field primaryIndexField = scheme.primaryIndexField();

        byte[] bytes = diskReader.read(pos, databaseConfiguration.diskPageSize());
        boolean isLeaf = bytes[0] == 1;
        byte[] amountOfEntriesBytes = new byte[Integer.BYTES];
        System.arraycopy(bytes, Byte.BYTES, amountOfEntriesBytes, 0, Integer.BYTES);
        int amountOfEntries = bytesManipulator.bytesToInt(amountOfEntriesBytes);

        if (amountOfEntries == 0) {
            return new TreeNode(pos, new Key[pLeaf], new byte[pLeaf][scheme.dataSize()], true, 0, -1);
        }

        if (!isLeaf) {
            Key[] keys = new Key[this.pNonLeaf];
            int[] p = new int[this.pNonLeaf + 1];

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

                int posOfIndex = bytesManipulator.bytesToInt(entryPosBytes) - pos;
                System.arraycopy(bytes, posOfIndex, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentIndex = bytesManipulator.bytesToInt(tmpByteBuffer);

                System.arraycopy(bytes, posOfIndex + Integer.BYTES, tmpByteBuffer, 0, Integer.BYTES);
                p[i] = bytesManipulator.bytesToInt(tmpByteBuffer);
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
                    p[i + 1] = bytesManipulator.bytesToInt(tmpByteBuffer);
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

            byte[][] values = new byte[this.pLeaf][scheme.dataSize()];
            int next = -1;

            byte[] entryPosBytes = new byte[Integer.BYTES];
            byte[] tmpByteBuffer = new byte[Integer.BYTES];
            int sizeOfCurrentIndex, sizeOfCurrentData;

            for (int i = 0; i < amountOfEntries; ++i) {
                System.arraycopy(
                        bytes, reservedSpaceInNode() + Integer.BYTES * i, entryPosBytes, 0, Integer.BYTES);

                int posOfIndex = bytesManipulator.bytesToInt(entryPosBytes) - pos;
                System.arraycopy(bytes, posOfIndex, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentIndex = bytesManipulator.bytesToInt(tmpByteBuffer);

                System.arraycopy(bytes, posOfIndex + Integer.BYTES, tmpByteBuffer, 0, Integer.BYTES);
                sizeOfCurrentData = bytesManipulator.bytesToInt(tmpByteBuffer);

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
                    next = bytesManipulator.bytesToInt(tmpByteBuffer);
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

    void saveNode(final int pos, final TreeNode node) throws IOException {
        if (!node.leaf) {
            int estimatedSize = estimateSizeOfElementsInNonLeafNode(scheme);
            checkNodeSize(node.q, estimatedSize);

            int currPos = pos;
            diskWriter.write(currPos, new byte[]{0});
            currPos++;
            diskWriter.write(currPos, bytesManipulator.intToBytes(node.q));

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
                            bytesManipulator.intToBytes(node.p[node.q]));
                }
            }
        } else {
            int estimatedSize = estimateSizeOfElementsInLeafNode(scheme);
            checkNodeSize(node.q, estimatedSize);

            int currPos = pos;
            diskWriter.write(currPos, new byte[]{1});
            currPos++;
            diskWriter.write(currPos, bytesManipulator.intToBytes(node.q));

            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                int posOfIndex = pos + reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * dataNonVariableRecordSize();

                diskWriter.write(currPos, bytesManipulator.intToBytes(posOfIndex));
                diskWriter.write(posOfIndex, bytesManipulator.intToBytes(scheme.primaryIndexSize()));
                diskWriter.write(posOfIndex + Integer.BYTES, bytesManipulator.intToBytes(scheme.dataSize()));

                diskWriter.write(posOfIndex + 2 * Integer.BYTES, node.keys[i].field().value());
                diskWriter.write(posOfIndex + 2 * Integer.BYTES + scheme.primaryIndexSize(), node.values[i]);

                if (i == node.q - 1) {
                    diskWriter.write(posOfIndex + 2 * Integer.BYTES + scheme.primaryIndexSize() + scheme.dataSize(),
                            bytesManipulator.intToBytes(node.nextPos));
                }
            }
        }
    }

    private void checkNodeSize(int size, int availableSize) {
        if (availableSize < size) {
            throw new IllegalArgumentException("Node size can't be longer than size of disk page");
        }
    }

    <T> void insertInArray(T[] array, T value, int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    void insertInArray(int[] array, int value, int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    <T> void copyArray(T[] source, T[] destination, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    void copyArray(int[] source, int[] destination, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    <T> void copyArray(T[] source, T[] destination, int pos, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }

    void copyArray(int[] source, int[] destination, int pos, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }

    /**
     * Binary search key in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position of properly key
     */
    int search(Key[] keys, int keysLength, Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                throw new IllegalArgumentException();
            }
        }
        return l;
    }

    /**
     * Binary search key in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position of properly key
     */
    int searchKey(Key[] keys, int keysLength, Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return m;
            }
        }
        return l;
    }
    /**
     * Find properly position in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position for insert
     */
    int findProperlyPosition(Key[] keys, int keysLength, Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return -1;
            }
        }
        return l;
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
        return (int) bfr - 2; // saving space in the node for future entry updates
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
        int recordSize = primaryIndexNonVariableRecordSize()
                + Integer.BYTES;  // seek position

        double bfr = ((double) nodeSize - reservedSpace) / recordSize;
        return (int) bfr - 2; // saving space in the node for future entry updates
    }

    int dataNonVariableRecordSize() {
        return scheme.dataSize() +
                scheme.primaryIndexSize() +
                +Integer.BYTES // size of index
                + Integer.BYTES; // size of data
    }

    int primaryIndexNonVariableRecordSize() {
        return scheme.primaryIndexSize()
                + Integer.BYTES  // seek pointer size
                + Integer.BYTES; // size of index
    }

    int reservedSpaceInNode() {
        return Byte.BYTES + Integer.BYTES;
    }

    /**
     * Free space position
     *
     * @return position
     */
    int freeSpacePos() {
        return (numberOfUsedBlocks + 1) * databaseConfiguration.diskPageSize();
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
