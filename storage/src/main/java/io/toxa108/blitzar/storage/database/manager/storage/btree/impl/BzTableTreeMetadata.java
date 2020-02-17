package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.DiskReaderIo;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class BzTableTreeMetadata implements TableTreeMetadata {
    private final DatabaseConfiguration databaseConfiguration;
    private final Scheme scheme;
    private final DiskReader diskReader;
    private final int pLeaf;
    private final int pNonLeaf;

    public BzTableTreeMetadata(final File file,
                               final DatabaseConfiguration databaseConfiguration,
                               final Scheme scheme) throws IOException {
        this.databaseConfiguration = databaseConfiguration;
        this.scheme = scheme;
        this.diskReader = new DiskReaderIo(file);
        this.pLeaf = pLeafCalculate();
        this.pNonLeaf = pNonLeafCalculate();
    }

    /**
     * Estimate number of entries per leaf node
     *
     * @return number of entries
     */
    private int pLeafCalculate() {
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
     * Estimate number of entries per non leaf node
     *
     * @return number of entries
     */
    private int pNonLeafCalculate() {
        int reservedSpace = reservedSpaceInNode()
                + Integer.BYTES; // additional N child pointer
        int nodeSize = databaseConfiguration.diskPageSize();
        int recordSize = primaryIndexNonVariableRecordSize()
                + Integer.BYTES;  // seek position

        double bfr = ((double) nodeSize - reservedSpace) / recordSize;
        return (int) bfr - 2; // saving space in the node for future entry updates
    }

    @Override
    public DatabaseConfiguration databaseConfiguration() {
        return databaseConfiguration;
    }

    /**
     * Return reserved space in node
     * 1 byte - is node leaf or non leaf
     * 4 byte - number of entries in leaf
     *
     * @return reserved space in bytes
     */
    @Override
    public int reservedSpaceInNode() {
        return Byte.BYTES + Integer.BYTES;
    }

    /**
     * Return position to free space
     *
     * @return position to free space
     * @throws IOException disk is error
     */
    @Override
    public int freeSpacePos() throws IOException {
        return (numberOfUsedBlocks() + 1) * databaseConfiguration.diskPageSize() + databaseConfiguration.metadataSize();
    }

    /**
     * Data size for non variable record
     * @return data size for non variable record
     */
    @Override
    public int dataNonVariableRecordSize() {
        return scheme.dataSize()
                + scheme.primaryIndexSize()
                + Integer.BYTES // size of index
                + Integer.BYTES; // size of data
    }

    @Override
    public int primaryIndexNonVariableRecordSize() {
        return scheme.primaryIndexSize()
                + Integer.BYTES  // seek pointer size
                + Integer.BYTES; // size of index
    }

    /**
     * Works only for not variable rows
     * Return the amount of elements can be stored in the node
     *
     * @return number of elements
     */
    @Override
    public int entriesInLeafNodeNumber() {
        return pLeaf;
    }

    /**
     * Works only for not variable rows
     * Return the amount of elements can be stored in the node
     *
     * @return number of elements
     */
    @Override
    public int entriesInNonLeafNodeNumber() {
        return pNonLeaf;
    }

    @Override
    public int primaryIndexSize() {
        return scheme.primaryIndexSize();
    }

    @Override
    public int dataSize() {
        return scheme.dataSize();
    }

    @Override
    public Set<Field> dataFields() {
        return scheme.dataFields();
    }

    @Override
    public boolean containIndex(String indexName) {
        return scheme.containIndex(indexName);
    }

    @Override
    public Field primaryIndexField() {
        return scheme.primaryIndexField();
    }

    // TODO get rid from disk seeks each time
    @Override
    public int numberOfUsedBlocks() throws IOException {
        return BytesManipulator.bytesToInt(diskReader.read(0, Integer.BYTES));
    }
}
