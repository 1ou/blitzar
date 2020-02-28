package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeWriter;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.BzDiskWriterIo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BzDiskTreeWriter implements DiskTreeWriter {
    private final Logger log = LoggerFactory.getLogger(BzDiskTreeWriter.class);

    /**
     * Disk writer
     */
    private final DiskWriter diskWriter;

    /**
     * Table metadata
     */
    private final TableTreeMetadata tableTreeMetadata;

    /**
     * Array manipulator
     */
    private final ArrayManipulator arrayManipulator;

    public BzDiskTreeWriter(final File file,
                            final TableTreeMetadata tableTreeMetadata) throws IOException {
        this.diskWriter = new BzDiskWriterIo(file);
        this.tableTreeMetadata = tableTreeMetadata;
        this.arrayManipulator = new ArrayManipulator();
    }

    @Override
    public void write(final int pos, final TreeNode node) throws IOException {
        log.info("Write to: " + pos);

        final byte[] nodeBytes = new byte[tableTreeMetadata.databaseConfiguration().diskPageSize()];
        final int primaryIndexSize = tableTreeMetadata.primaryIndexSize();

        if (!node.leaf) {
            final int estimatedSize = tableTreeMetadata.entriesInNonLeafNodeNumber();
            checkNodeSize(node.q, estimatedSize);

            int currPos = 0;

            arrayManipulator.insertInArray(nodeBytes, (byte) 0, 0);
            System.arraycopy(BytesManipulator.intToBytes(node.q), 0, nodeBytes, ++currPos, Integer.BYTES);

            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                final int posOfIndex = pos + tableTreeMetadata.reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * tableTreeMetadata.primaryIndexNonVariableRecordSize();

                System.arraycopy(BytesManipulator.intToBytes(posOfIndex), 0, nodeBytes, currPos, Integer.BYTES);
                System.arraycopy(BytesManipulator.intToBytes(primaryIndexSize), 0, nodeBytes, posOfIndex - pos, Integer.BYTES);
                System.arraycopy(BytesManipulator.intToBytes(node.p[i]), 0, nodeBytes, posOfIndex + Integer.BYTES - pos, Integer.BYTES);

                final byte[] indexValue = node.keys[i].field().value();
                System.arraycopy(indexValue, 0, nodeBytes, (posOfIndex + 2 * Integer.BYTES) - pos, indexValue.length);

                if (i == node.q - 1) {
                    System.arraycopy(BytesManipulator.intToBytes(node.p[node.q]), 0, nodeBytes, (posOfIndex + 2 * Integer.BYTES + indexValue.length) - pos, Integer.BYTES);
                }
            }
        } else {
            final int estimatedSize = tableTreeMetadata.entriesInLeafNodeNumber();
            final int dataSize = tableTreeMetadata.dataSize();
            checkNodeSize(node.q, estimatedSize);

            int currPos = 0;
            arrayManipulator.insertInArray(nodeBytes, (byte) 1, 0);
            System.arraycopy(BytesManipulator.intToBytes(node.q), 0, nodeBytes, ++currPos, Integer.BYTES);

            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                int posOfIndex = pos + tableTreeMetadata.reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * tableTreeMetadata.dataNonVariableRecordSize();

                System.arraycopy(BytesManipulator.intToBytes(posOfIndex), 0, nodeBytes, currPos, Integer.BYTES);
                System.arraycopy(BytesManipulator.intToBytes(primaryIndexSize), 0, nodeBytes, posOfIndex - pos, Integer.BYTES);
                System.arraycopy(BytesManipulator.intToBytes(dataSize), 0, nodeBytes, posOfIndex + Integer.BYTES - pos, Integer.BYTES);

                System.arraycopy(node.keys[i].field().value(), 0, nodeBytes, posOfIndex + 2 * Integer.BYTES - pos, node.keys[i].field().value().length);
                System.arraycopy(node.values[i], 0, nodeBytes, (posOfIndex + 2 * Integer.BYTES + primaryIndexSize) - pos, node.values[i].length);

                if (i == node.q - 1) {
                    System.arraycopy(BytesManipulator.intToBytes(node.nextPos), 0, nodeBytes,
                            (posOfIndex + 2 * Integer.BYTES + primaryIndexSize + dataSize) - pos, Integer.BYTES);

                }
            }
        }
        diskWriter.write(pos, nodeBytes);
    }

    @Override
    public void updateMetadata(final int numberOfBlocks) throws IOException {
        diskWriter.write(0, BytesManipulator.intToBytes(numberOfBlocks));
    }

    private void checkNodeSize(final int size, final int availableSize) {
        if (availableSize < size) {
            throw new IllegalArgumentException("Node size can't be longer than size of disk page");
        }
    }
}
