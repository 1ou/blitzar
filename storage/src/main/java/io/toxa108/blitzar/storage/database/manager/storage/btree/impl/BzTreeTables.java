package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
import io.toxa108.blitzar.storage.database.manager.storage.Tables;
import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeReader;
import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeWriter;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.database.manager.storage.buffer.BufferPool;
import io.toxa108.blitzar.storage.database.manager.storage.buffer.BzBufferPool;
import io.toxa108.blitzar.storage.database.manager.transaction.BzTableTableLocks;
import io.toxa108.blitzar.storage.database.manager.transaction.TableLocks;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.impl.BzField;
import io.toxa108.blitzar.storage.database.schema.impl.BzKey;
import io.toxa108.blitzar.storage.database.schema.impl.BzRow;
import io.toxa108.blitzar.storage.database.schema.transform.impl.FieldValueAsString;
import io.toxa108.blitzar.storage.io.impl.DiskNioReader;
import io.toxa108.blitzar.storage.io.impl.DiskNioWriter;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Threadsafe b-plus-tree on disk realization
 */
public class BzTreeTables implements Tables {
    private int numberOfUsedBlocks = 0;
    private final SearchKeys searchKeys;
    private final TableLocks tableLocks;
    private final TableTreeMetadata tableMetadata;
    private final DiskTreeReader diskTreeReader;
    private final DiskTreeWriter diskTreeWriter;
    private final BufferPool bufferPool;
    private final int pNonLeaf, pLeaf;

    public BzTreeTables(final File file,
                        final DatabaseConfiguration databaseConfiguration,
                        final Scheme scheme) {
        try {
            this.searchKeys = new SearchKeys();
            this.tableLocks = new BzTableTableLocks();
            this.tableMetadata = new BzTableTreeMetadata(file, databaseConfiguration, scheme);
            this.diskTreeReader = new BzDiskTreeReader(file, tableMetadata);
            this.diskTreeWriter = new BzDiskTreeWriter(file, tableMetadata);

            this.bufferPool = new BzBufferPool(
                    new DiskNioReader(file),
                    new DiskNioWriter(file),
                    databaseConfiguration
            );

            this.pLeaf = tableMetadata.entriesInLeafNodeNumber();
            this.pNonLeaf = tableMetadata.entriesInNonLeafNodeNumber();

        } catch (IOException e) {
            throw new IllegalArgumentException("Runtime error. The blitzar is not configured.");
        }
    }

    void insertEntryInRootLeafNoSplit(final TreeNode n,
                                      final Row row,
                                      final int pos) throws IOException {
        if (n.q == 0) {
            numberOfUsedBlocks = 1;
            diskTreeWriter.updateMetadata(numberOfUsedBlocks);
        }

        ArrayManipulator.insertInArray(n.keys, row.key(), pos);
        ArrayManipulator.insertInArray(n.p, -1, pos);
        n.q++;
        ArrayManipulator.insertInArray(
                n.values,
                rowDataToBytes(row),
                pos
        );

        diskTreeWriter.write(n.pos, n);
    }

    void insertEntryInRootLeafWithSplit(final TreeNode n,
                                        final int newNodePos,
                                        final Key key,
                                        final int tmpPosition) throws IOException {
        final TreeNode topNode = new TreeNode(pLeaf, tableMetadata.dataSize());
        topNode.keys[0] = key;
        topNode.p[0] = n.pos;
        topNode.p[1] = newNodePos;//newNode.pos;
        topNode.leaf = false;
        topNode.q = 1;

        diskTreeWriter.write(tmpPosition, topNode);
        diskTreeWriter.updateMetadata(numberOfUsedBlocks++);
    }

    /**
     * Return position
     *
     * @param stack stack
     * @param key   key
     * @return position
     * @throws IOException disk io issue
     */
    int traverseDown(final Stack<Integer> stack,
                     final Key key) throws IOException {
        final int beginPos = tableMetadata.databaseConfiguration().metadataSize() + 1;

        TreeNode n = diskTreeReader.read(beginPos);

        while (!n.leaf) {
            stack.push(n.pos);
            final int q = n.q;
            if (key.compareTo(n.keys[0]) < 0) {
                tableLocks.shared(n.p[0]);
                n = diskTreeReader.read(n.p[0]);
            } else if (key.compareTo(n.keys[q - 1]) > 0) {
                tableLocks.shared(n.p[q]);
                n = diskTreeReader.read(n.p[q]);
            } else {
                final int fn = searchKeys.findProperlyPosition(n.keys, n.q, key);
                if (fn == -1) {
                    throw new IllegalArgumentException("Row is not inserted. Error.");
                }
                tableLocks.shared(n.p[q]);
                n = diskTreeReader.read(n.p[fn]);
            }
        }
        return n.pos;
    }

    static class SplitMetadata {
        int pos;
        Key key;

        public SplitMetadata(int pos, Key key) {
            this.pos = pos;
            this.key = key;
        }
    }

    SplitMetadata splitBeforeInsert(final TreeNode n,
                                    final TreeNode newNode,
                                    final Row row,
                                    final int properlyPosition) throws IOException {
        int tmpPosition = n.pos;
        TreeNode tmp = new TreeNode(pLeaf + 1, tableMetadata.dataSize());
        ArrayManipulator.copyArray(n.keys, tmp.keys, n.q);
        ArrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
        ArrayManipulator.copyArray(n.values, tmp.values, n.q);
        ArrayManipulator.insertInArray(tmp.keys, row.key(), properlyPosition);
        ArrayManipulator.insertInArray(tmp.p, numberOfUsedBlocks, properlyPosition);
        ArrayManipulator.insertInArray(tmp.values, rowDataToBytes(row), properlyPosition);
        tmp.q = n.q + 1;

        newNode.nextPos = n.nextPos;
        int j = (pLeaf + 1) >>> 1;

        n.keys = new Key[pLeaf];
        n.p = new int[pLeaf + 1];
        ArrayManipulator.copyArray(tmp.keys, n.keys, j);
        ArrayManipulator.copyArray(tmp.values, n.values, j);
        ArrayManipulator.copyArray(tmp.p, n.p, j);
        n.q = j;

        ArrayManipulator.copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
        ArrayManipulator.copyArray(tmp.values, newNode.values, j, tmp.q - j);
        ArrayManipulator.copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
        newNode.q = tmp.q - j;
        Key key = tmp.keys[j - 1];

        int newPosLeft = tableMetadata.freeSpacePos();
        int newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

        n.nextPos = newPosRight;
        n.pos = newPosLeft;
        newNode.pos = newPosRight;

        diskTreeWriter.write(newPosLeft, n);
        diskTreeWriter.write(newPosRight, newNode);
        numberOfUsedBlocks += 2;
        diskTreeWriter.updateMetadata(numberOfUsedBlocks);

        return new SplitMetadata(tmpPosition, key);
    }

    void insertInInternalNodeNoSplit(final int pos,
                                     final TreeNode newNode,
                                     final Key key) throws IOException {
        final TreeNode n = diskTreeReader.read(pos);
        final int properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
        ArrayManipulator.insertInArray(n.keys, key, properlyPosition);
        ArrayManipulator.insertInArray(n.p, newNode.pos, properlyPosition + 1);
        n.q++;

        diskTreeWriter.write(n.pos, n);
        diskTreeWriter.updateMetadata(numberOfUsedBlocks);
    }

    @Override
    public void addRow(final Row row) throws IOException {
        final Stack<Integer> positions = new Stack<>();
        final int pLeaf = tableMetadata.entriesInLeafNodeNumber();
        final int pNonLeaf = tableMetadata.entriesInNonLeafNodeNumber();

        Key key = row.key();
        int pos = traverseDown(positions, key);

        tableLocks.exclusive(pos);
        TreeNode n = diskTreeReader.read(pos);

        int properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
        /*
            If record with such key already exists
         */
        if (properlyPosition == -1) {
            throw new IllegalArgumentException(
                    "key: " + key.field().name() + " was inserted into node [" +
                            Arrays.stream(n.keys).map(it -> it.field().name())
                                    .collect(Collectors.joining(", ")) + "]");
        } else {
            TreeNode newNode = new TreeNode(pLeaf, tableMetadata.dataSize());
            /*
                If leaf is not full, insert new entry in leaf
             */
            if (n.q < pLeaf - 1) {
                insertEntryInRootLeafNoSplit(n, row, properlyPosition);
                tableLocks.unexclusive(n.pos);
            }
            /*
                Split leaf before insert
             */
            else {
                final SplitMetadata splitMetadata = splitBeforeInsert(n, newNode, row, properlyPosition);
                int tmpPosition = splitMetadata.pos;
                key = splitMetadata.key;

                boolean finished = false;
                while (!finished) {
                    if (positions.isEmpty()) {
                        insertEntryInRootLeafWithSplit(
                                n,
                                newNode.pos,
                                key,
                                tmpPosition
                        );
                        tableLocks.unexclusive(pos);
                        finished = true;
                    } else {
                        int k = positions.pop();
                        n = diskTreeReader.read(k);
                        /*
                            If internal node n is not full
                            parent node is not full - no split
                        */
                        if (n.q < pNonLeaf - 1) {
                            insertInInternalNodeNoSplit(n.pos, newNode, key);
                            finished = true;
                        } else {
                            TreeNode tmp = new TreeNode(pNonLeaf + 1, tableMetadata.dataSize());
                            ArrayManipulator.copyArray(n.keys, tmp.keys, n.q);
                            ArrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
                            tmp.q = n.q + 1;

                            properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
                            ArrayManipulator.insertInArray(tmp.keys, key, properlyPosition);
                            ArrayManipulator.insertInArray(tmp.p, newNode.pos, properlyPosition + 1);

                            newNode = new TreeNode(pNonLeaf, tableMetadata.dataSize());
                            int j = (pNonLeaf + 1) >>> 1;

                            n.keys = new Key[pNonLeaf];
                            n.p = new int[pNonLeaf + 1];
                            ArrayManipulator.copyArray(tmp.keys, n.keys, 0, j);
                            ArrayManipulator.copyArray(tmp.p, n.p, 0, j);
                            n.q = j;
                            n.leaf = false;

                            ArrayManipulator.copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
                            ArrayManipulator.copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
                            newNode.q = tmp.q - j;
                            newNode.leaf = false;

                            int newPosLeft = tableMetadata.freeSpacePos();
                            int newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

                            tmpPosition = n.pos;
                            n.pos = newPosLeft;
                            newNode.pos = newPosRight;

                            diskTreeWriter.write(newPosLeft, n);
                            diskTreeWriter.write(newPosRight, newNode);
                            numberOfUsedBlocks += 2;
                            diskTreeWriter.updateMetadata(numberOfUsedBlocks);

                            key = tmp.keys[j - 1];
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<Row> search(final Field field) throws IOException {
        if (tableMetadata.containIndex(field.name())) {
            return search(new BzKey(field));
        }

        List<Row> foundedRows = new ArrayList<>();
        TreeNode n = diskTreeReader.read(tableMetadata.databaseConfiguration().metadataSize() + 1);

        while (!n.leaf) {
            n = diskTreeReader.read(n.p[0]);
        }

        while (true) {
            for (int i = 0; i < n.q; ++i) {
                byte[] data = n.values[i];
                AtomicInteger seek = new AtomicInteger();
                Set<Field> fields = tableMetadata.dataFields()
                        .stream()
                        .map(it -> {
                            int s = seek.getAndAdd(it.diskSize());
                            byte[] bytes = new byte[it.diskSize()];
                            System.arraycopy(data, s, bytes, 0, it.diskSize());
                            return new BzField(
                                    it.name(),
                                    it.type(),
                                    it.nullable(),
                                    it.unique(),
                                    bytes
                            );
                        })
                        .collect(Collectors.toSet());

                if (fields.stream().anyMatch(
                        it -> it.name().equals(field.name()) && Arrays.equals(it.value(), field.value()))) {
                    foundedRows.add(new BzRow(
                            n.keys[i],
                            fields
                    ));
                }
            }

            if (n.nextPos == -1) {
                break;
            }
            n = diskTreeReader.read(n.nextPos);
        }
        return foundedRows;
    }

    public List<Row> search(final Key key) throws IOException {
        TreeNode n = diskTreeReader.read(tableMetadata.databaseConfiguration().metadataSize() + 1);

        while (!n.leaf) {
            int q = n.q;
            if (key.compareTo(n.keys[0]) < 0) {
                n = diskTreeReader.read(n.p[0]);
            } else if (key.compareTo(n.keys[q - 1]) > 0) {
                n = diskTreeReader.read(n.p[q]);
            } else {
                int fn = searchKeys.searchTraverseWay(n.keys, n.q, key);
                n = diskTreeReader.read(n.p[fn]);
            }
        }
        final int i = searchKeys.searchKeyInNode(n.keys, n.q, key);
        if (i == -1) {
            throw new NoSuchElementException("Element with key " + new FieldValueAsString(key.field()).transform() + " is not found");
        }

        byte[] data = n.values[i];
        AtomicInteger seek = new AtomicInteger();
        Set<Field> fields = tableMetadata.dataFields()
                .stream()
                .map(it -> {
                    int s = seek.getAndAdd(it.diskSize());
                    byte[] bytes = new byte[it.diskSize()];
                    System.arraycopy(data, s, bytes, 0, it.diskSize());
                    return new BzField(
                            it.name(),
                            it.type(),
                            it.nullable(),
                            it.unique(),
                            bytes
                    );
                })
                .collect(Collectors.toSet());
        return List.of(new BzRow(
                n.keys[i],
                fields
        ));
    }

    @Override
    public List<Row> search() throws IOException {
        final List<Row> foundedRows = new ArrayList<>();
        TreeNode n = diskTreeReader.read(tableMetadata.databaseConfiguration().metadataSize() + 1);

        while (!n.leaf) {
            n = diskTreeReader.read(n.p[0]);
        }

        while (true) {
            for (int i = 0; i < n.q; ++i) {
                byte[] data = n.values[i];
                final AtomicInteger seek = new AtomicInteger();
                final Set<Field> fields = tableMetadata.dataFields()
                        .stream()
                        .map(it -> {
                            int s = seek.getAndAdd(it.diskSize());
                            byte[] bytes = new byte[it.diskSize()];
                            System.arraycopy(data, s, bytes, 0, it.diskSize());
                            return new BzField(
                                    it.name(),
                                    it.type(),
                                    it.nullable(),
                                    it.unique(),
                                    bytes
                            );
                        })
                        .collect(Collectors.toSet());
                foundedRows.add(new BzRow(
                        n.keys[i],
                        fields
                ));
            }

            if (n.nextPos == -1) {
                break;
            }
            n = diskTreeReader.read(n.nextPos);
        }

        return foundedRows;
    }

    /**
     * Convert data from row to bytes
     *
     * @param row row
     * @return byte array
     */
    private byte[] rowDataToBytes(final Row row) {
        return row.dataFields()
                .stream()
                .map(Field::value)
                .reduce((a, b) -> {
                    byte[] c = new byte[a.length + b.length];
                    System.arraycopy(a, 0, c, 0, a.length);
                    System.arraycopy(b, 0, c, a.length, b.length);
                    return c;
                })
                .orElse(new byte[0]);
    }
}
