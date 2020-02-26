package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
import io.toxa108.blitzar.storage.database.manager.storage.Tables;
import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeReader;
import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeWriter;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Threadsafe b-plus-tree on disk realization
 */
public class BzTreeTables implements Tables {
    private final ArrayManipulator arrayManipulator;
    private int numberOfUsedBlocks = 0;
    private final SearchKeys searchKeys;
    private final TableLocks tableLocks;
    private final TableTreeMetadata tableMetadata;
    private final DiskTreeReader diskTreeReader;
    private final DiskTreeWriter diskTreeWriter;

    public BzTreeTables(final File file,
                        final DatabaseConfiguration databaseConfiguration,
                        final Scheme scheme) {
        try {
            this.arrayManipulator = new ArrayManipulator();
            this.searchKeys = new SearchKeys();
            this.tableLocks = new BzTableTableLocks();
            this.tableMetadata = new BzTableTreeMetadata(file, databaseConfiguration, scheme);
            this.diskTreeReader = new BzDiskTreeReader(file, tableMetadata);
            this.diskTreeWriter = new BzDiskTreeWriter(file, tableMetadata);
        } catch (IOException e) {
            throw new IllegalArgumentException("Runtime error. The blitzar is not configured.");
        }
    }

    @Override
    public void addRow(final Row row) throws IOException {
        final int pLeaf = tableMetadata.entriesInLeafNodeNumber();
        final int pNonLeaf = tableMetadata.entriesInNonLeafNodeNumber();
        final List<Integer> exclusiveLocks = new ArrayList<>();
        final List<Integer> sharedLocks = new ArrayList<>();
        try {

            final int beginPos = tableMetadata.databaseConfiguration().metadataSize() + 1;
            tableLocks.shared(beginPos);
            sharedLocks.add(beginPos);

            TreeNode n = diskTreeReader.read(beginPos);
            Key key = row.key();

            final Stack<TreeNode> stack = new Stack<>();

            while (!n.leaf) {
                stack.push(n);
                final int q = n.q;
                if (key.compareTo(n.keys[0]) < 0) {
                    n = diskTreeReader.read(n.p[0]);
                } else if (key.compareTo(n.keys[q - 1]) > 0) {
                    n = diskTreeReader.read(n.p[q]);
                } else {
                    final int fn = searchKeys.findProperlyPosition(n.keys, n.q, key);
                    if (fn == -1) {
                        throw new IllegalArgumentException("Row is not inserted. Error.");
                    }
                    n = diskTreeReader.read(n.p[fn]);
                }
                tableLocks.shared(n.pos);
                sharedLocks.add(n.pos);
            }

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
                    sharedLocks.remove(Integer.valueOf(n.pos));

                    tableLocks.exclusive(n.pos);
                    exclusiveLocks.add(n.pos);

                    if (n.q == 0) {
                        numberOfUsedBlocks = 1;
                        diskTreeWriter.updateMetadata(numberOfUsedBlocks);
                    }

                    arrayManipulator.insertInArray(n.keys, key, properlyPosition);
                    arrayManipulator.insertInArray(n.p, -1, properlyPosition);
                    n.q++;
                    arrayManipulator.insertInArray(
                            n.values,
                            rowDataToBytes(row),
                            properlyPosition
                    );

                    diskTreeWriter.write(n.pos, n);

                    tableLocks.unexclusive(n.pos);
                    exclusiveLocks.remove(Integer.valueOf(n.pos));
                }
            /*
                Split leaf before insert
             */
                else {
                    int tmpPosition = n.pos;
                    TreeNode tmp = new TreeNode(pLeaf + 1, tableMetadata.dataSize());
                    arrayManipulator.copyArray(n.keys, tmp.keys, n.q);
                    arrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
                    arrayManipulator.copyArray(n.values, tmp.values, n.q);
                    arrayManipulator.insertInArray(tmp.keys, key, properlyPosition);
                    arrayManipulator.insertInArray(tmp.p, numberOfUsedBlocks, properlyPosition);
                    arrayManipulator.insertInArray(tmp.values, rowDataToBytes(row), properlyPosition);
                    tmp.q = n.q + 1;

                    newNode.nextPos = n.nextPos;
                    int j = (pLeaf + 1) >>> 1;

                    n.keys = new Key[pLeaf];
                    n.p = new int[pLeaf + 1];
                    arrayManipulator.copyArray(tmp.keys, n.keys, j);
                    arrayManipulator.copyArray(tmp.values, n.values, j);
                    arrayManipulator.copyArray(tmp.p, n.p, j);
                    n.q = j;

                    arrayManipulator.copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
                    arrayManipulator.copyArray(tmp.values, newNode.values, j, tmp.q - j);
                    arrayManipulator.copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
                    newNode.q = tmp.q - j;
                    key = tmp.keys[j - 1];

                    int newPosLeft = tableMetadata.freeSpacePos();
                    int newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

                    n.nextPos = newPosRight;
                    n.pos = newPosLeft;
                    newNode.pos = newPosRight;

                    tableLocks.exclusive(newPosLeft);
                    exclusiveLocks.add(newPosLeft);
                    tableLocks.exclusive(newPosRight);
                    exclusiveLocks.add(newPosRight);

                    diskTreeWriter.write(newPosLeft, n);
                    diskTreeWriter.write(newPosRight, newNode);
                    numberOfUsedBlocks += 2;
                    diskTreeWriter.updateMetadata(numberOfUsedBlocks);

                    tableLocks.unexclusive(newPosRight);
                    exclusiveLocks.remove(Integer.valueOf(newPosRight));
                    tableLocks.unexclusive(newPosLeft);
                    exclusiveLocks.remove(Integer.valueOf(newPosLeft));

                    boolean finished = false;
                    while (!finished) {
                        if (stack.isEmpty()) {
                            TreeNode topNode = new TreeNode(pLeaf, tableMetadata.dataSize());
                            topNode.keys[0] = key;
                            topNode.p[0] = n.pos;
                            topNode.p[1] = newNode.pos;
                            topNode.leaf = false;
                            topNode.q = 1;

                            tableLocks.exclusive(tmpPosition);
                            exclusiveLocks.add(tmpPosition);

                            diskTreeWriter.write(tmpPosition, topNode);
                            numberOfUsedBlocks += 1;
                            diskTreeWriter.updateMetadata(numberOfUsedBlocks);

                            tableLocks.unexclusive(newPosRight);
                            exclusiveLocks.remove(Integer.valueOf(newPosRight));
                            finished = true;
                        } else {
                            /*
                             * We have to made all shared lock - exclusive
                             */
                            if (n.leaf) {
                                sharedLocks.forEach(it -> {
                                    sharedLocks.remove(it);
                                    tableLocks.unshared(it);

                                    exclusiveLocks.add(it);
                                    tableLocks.exclusive(it);
                                });
                            }
                            n = stack.pop();
                            /*
                                If internal node n is not full
                                parent node is not full - no split
                             */
                            if (n.q < pNonLeaf - 1) {
                                properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
                                arrayManipulator.insertInArray(n.keys, key, properlyPosition);
                                arrayManipulator.insertInArray(n.p, newNode.pos, properlyPosition + 1);
                                n.q++;

                                tableLocks.exclusive(n.pos);
                                exclusiveLocks.add(n.pos);

                                diskTreeWriter.write(n.pos, n);
                                diskTreeWriter.updateMetadata(numberOfUsedBlocks);

                                tableLocks.unexclusive(n.pos);
                                exclusiveLocks.remove(Integer.valueOf(n.pos));

                                finished = true;
                            } else {
                                tmp = new TreeNode(pNonLeaf + 1, tableMetadata.dataSize());
                                arrayManipulator.copyArray(n.keys, tmp.keys, n.q);
                                arrayManipulator.copyArray(n.p, tmp.p, n.q + 1);
                                tmp.q = n.q + 1;

                                properlyPosition = searchKeys.findProperlyPosition(n.keys, n.q, key);
                                arrayManipulator.insertInArray(tmp.keys, key, properlyPosition);
                                arrayManipulator.insertInArray(tmp.p, newNode.pos, properlyPosition + 1);

                                newNode = new TreeNode(pNonLeaf, tableMetadata.dataSize());
                                j = (pNonLeaf + 1) >>> 1;

                                n.keys = new Key[pNonLeaf];
                                n.p = new int[pNonLeaf + 1];
                                arrayManipulator.copyArray(tmp.keys, n.keys, 0, j);
                                arrayManipulator.copyArray(tmp.p, n.p, 0, j);
                                n.q = j;
                                n.leaf = false;

                                arrayManipulator.copyArray(tmp.keys, newNode.keys, j, tmp.q - j);
                                arrayManipulator.copyArray(tmp.p, newNode.p, j, tmp.q - j + 1);
                                newNode.q = tmp.q - j;
                                newNode.leaf = false;

                                newPosLeft = tableMetadata.freeSpacePos();
                                newPosRight = tableMetadata.freeSpacePos() + tableMetadata.databaseConfiguration().diskPageSize();

                                tmpPosition = n.pos;
                                n.pos = newPosLeft;
                                newNode.pos = newPosRight;

                                tableLocks.exclusive(newPosLeft);
                                exclusiveLocks.add(newPosLeft);

                                tableLocks.exclusive(newPosRight);
                                exclusiveLocks.add(newPosRight);

                                diskTreeWriter.write(newPosLeft, n);
                                diskTreeWriter.write(newPosRight, newNode);
                                numberOfUsedBlocks += 2;
                                diskTreeWriter.updateMetadata(numberOfUsedBlocks);

                                tableLocks.unexclusive(newPosRight);
                                exclusiveLocks.remove(Integer.valueOf(newPosRight));

                                tableLocks.unexclusive(newPosLeft);
                                exclusiveLocks.remove(Integer.valueOf(newPosLeft));

                                key = tmp.keys[j - 1];
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
              sharedLocks.forEach(tableLocks::unshared);
              exclusiveLocks.forEach(tableLocks::unexclusive);
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
