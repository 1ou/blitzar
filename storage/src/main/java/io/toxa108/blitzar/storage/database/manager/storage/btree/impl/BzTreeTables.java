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
            this.tableMetadata = new BzTableTreeMetadata(file, databaseConfiguration, scheme);
            this.diskTreeReader = new BzDiskTreeReader(file, tableMetadata);
            this.diskTreeWriter = new BzDiskTreeWriter(file, tableMetadata);
            this.tableLocks = new BzTableTableLocks(tableMetadata.name());

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

    void insertEntryInRootNonLeafWithSplit(final TreeNode n,
                                           final int newNodePos,
                                           final Key key,
                                           final int tmpPosition) throws IOException {
        final TreeNode topNode = new TreeNode(pLeaf, tableMetadata.dataSize());
        topNode.keys[0] = key;
        topNode.p[0] = n.pos;
        topNode.p[1] = newNodePos;
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
                     final Key key,
                     final List<Integer> exclusive,
                     final List<Integer> shared) throws IOException {
        final int beginPos = tableMetadata.databaseConfiguration().metadataSize() + 1;
        tableLocks.exclusive(beginPos);
        exclusive.add(beginPos);
        TreeNode n = diskTreeReader.read(beginPos);

        while (!n.leaf) {
            stack.push(n.pos);
            final int q = n.q;
            if (key.compareTo(n.keys[0]) < 0) {
                if (n.q < pNonLeaf - 1) {
                    tableLocks.shared(n.p[0]);
                    shared.add(n.p[0]);
                } else {
                    tableLocks.exclusive(n.p[0]);
                    exclusive.add(n.p[0]);
                }
                n = diskTreeReader.read(n.p[0]);
            } else if (key.compareTo(n.keys[q - 1]) > 0) {
                if (n.q < pNonLeaf - 1) {
                    tableLocks.shared(n.p[q]);
                    shared.add(n.p[q]);
                } else {
                    tableLocks.exclusive(n.p[q]);
                    exclusive.add(n.p[q]);
                }
                n = diskTreeReader.read(n.p[q]);
            } else {
                final int fn = searchKeys.findProperlyPosition(n.keys, n.q, key);
                if (fn == -1) {
                    throw new IllegalArgumentException("Row is not inserted. Error.");
                }
                if (n.q < pNonLeaf - 1) {
                    tableLocks.shared(n.p[fn]);
                    shared.add(n.p[fn]);
                } else {
                    tableLocks.exclusive(n.p[fn]);
                    exclusive.add(n.p[fn]);
                }
                n = diskTreeReader.read(n.p[fn]);
            }
        }
        return n.pos;
    }

    static class SplitLeafMetadata {
        int pos;
        Key key;

        public SplitLeafMetadata(int pos, Key key) {
            this.pos = pos;
            this.key = key;
        }
    }

    SplitLeafMetadata splitLeafBeforeInsert(final TreeNode n,
                                            final TreeNode newNode,
                                            final Row row,
                                            final int properlyPosition) throws IOException {
        final int tmpPosition = n.pos;

        final TreeNode tmp = new TreeNode(pLeaf + 1, tableMetadata.dataSize());
        ArrayManipulator.copyArray(n.keys, tmp.keys, n.q);
        ArrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
        ArrayManipulator.copyArray(n.values, tmp.values, n.q);
        ArrayManipulator.insertInArray(tmp.keys, row.key(), properlyPosition);
        ArrayManipulator.insertInArray(tmp.p, numberOfUsedBlocks, properlyPosition);
        ArrayManipulator.insertInArray(tmp.values, rowDataToBytes(row), properlyPosition);
        tmp.q = n.q + 1;

        newNode.nextPos = n.nextPos;
        final int j = (pLeaf + 1) >>> 1;

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
        final Key key = tmp.keys[j - 1];

        final int newPosLeft = tableMetadata.freeSpacePos();
        final int newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

        n.nextPos = newPosRight;
        n.pos = newPosLeft;
        newNode.pos = newPosRight;

        diskTreeWriter.write(newPosLeft, n);
        diskTreeWriter.write(newPosRight, newNode);
        numberOfUsedBlocks += 2;
        diskTreeWriter.updateMetadata(numberOfUsedBlocks);

        return new SplitLeafMetadata(tmpPosition, key);
    }

    static class SplitNonLeafMetadata {
        final int tmpPosition;
        final TreeNode newestNode;
        final Key key;

        SplitNonLeafMetadata(final int tmpPosition, final TreeNode newestNode, final Key key) {
            this.tmpPosition = tmpPosition;
            this.newestNode = newestNode;
            this.key = key;
        }
    }

    SplitNonLeafMetadata splitNonLeafBeforeInsert(final TreeNode n,
                                                  final TreeNode newNode,
                                                  final Key key) throws IOException {
        final TreeNode tmp = new TreeNode(pNonLeaf + 1, tableMetadata.dataSize());
        ArrayManipulator.copyArray(n.keys, tmp.keys, n.q);
        ArrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
        tmp.q = n.q + 1;

        final int properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
        ArrayManipulator.insertInArray(tmp.keys, key, properlyPosition);
        ArrayManipulator.insertInArray(tmp.p, newNode.pos, properlyPosition + 1);

        final TreeNode newestNode = new TreeNode(pNonLeaf, tableMetadata.dataSize());

        final int j = (pNonLeaf + 1) >>> 1;

        n.keys = new Key[pNonLeaf];
        n.p = new int[pNonLeaf + 1];
        ArrayManipulator.copyArray(tmp.keys, n.keys, 0, j);
        ArrayManipulator.copyArray(tmp.p, n.p, 0, j);
        n.q = j;
        n.leaf = false;

        ArrayManipulator.copyArray(tmp.keys, newestNode.keys, j, tmp.q - j);
        ArrayManipulator.copyArray(tmp.p, newestNode.p, j, tmp.q - j + 1);
        newestNode.q = tmp.q - j;
        newestNode.leaf = false;

        final int newPosLeft = tableMetadata.freeSpacePos();
        final int newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

        final int tmpPosition = n.pos;
        n.pos = newPosLeft;
        newestNode.pos = newPosRight;

        diskTreeWriter.write(newPosLeft, n);
        diskTreeWriter.write(newPosRight, newestNode);
        numberOfUsedBlocks += 2;
        diskTreeWriter.updateMetadata(numberOfUsedBlocks);

        return new SplitNonLeafMetadata(
                tmpPosition,
                newestNode,
                tmp.keys[j - 1]
        );
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
        final List<Integer> exclusive = new ArrayList<>();
        final List<Integer> shared = new ArrayList<>();

        try {
            Key key = row.key();
            final int pos = traverseDown(positions, key, exclusive, shared);

            tableLocks.exclusive(pos);
            exclusive.add(pos);
            TreeNode n = diskTreeReader.read(pos);

            int properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
            /*
                If record with such key already exists
             */
            if (properlyPosition == -1) {
                throw new IllegalArgumentException(
                        "key: " + key.field().name() + " has been already contained in table [" +
                                Arrays.stream(n.keys).map(it -> it.field().name())
                                        .collect(Collectors.joining(", ")) + "]");
            } else {
                TreeNode newNode = new TreeNode(pLeaf, tableMetadata.dataSize());
            /*
                If leaf is not full, insert new entry in leaf
             */
                if (n.q < pLeaf - 1) {
                    insertEntryInRootLeafNoSplit(n, row, properlyPosition);
                }
            /*
                Split leaf before insert
             */
                else {
                    final SplitLeafMetadata splitLeafMetadata = splitLeafBeforeInsert(n, newNode, row, properlyPosition);
                    int tmpPosition = splitLeafMetadata.pos;
                    key = splitLeafMetadata.key;

                    boolean finished = false;
                    while (!finished) {
                        if (positions.isEmpty()) {
                            insertEntryInRootNonLeafWithSplit(
                                    n,
                                    newNode.pos,
                                    key,
                                    tmpPosition
                            );
                            finished = true;
                        } else {
                            /*
                             * We have to lock all stack positions exclusive
                             */
                            final int k = positions.pop();
                            n = diskTreeReader.read(k);
                        /*
                            If internal node n is not full
                            parent node is not full - no split
                        */
                            if (n.q < pNonLeaf - 1) {
                                insertInInternalNodeNoSplit(n.pos, newNode, key);
                                finished = true;
                            } else {
                                SplitNonLeafMetadata splitNonLeafBeforeInsert = splitNonLeafBeforeInsert(
                                        n,
                                        newNode,
                                        key
                                );
                                tmpPosition = splitNonLeafBeforeInsert.tmpPosition;
                                newNode = splitNonLeafBeforeInsert.newestNode;
                                key = splitNonLeafBeforeInsert.key;
                            }
                        }
                    }
                }
            }
        } finally {
            shared.forEach(tableLocks::unshared);
            exclusive.forEach(tableLocks::unexclusive);
            exclusive.clear();
            shared.clear();
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
