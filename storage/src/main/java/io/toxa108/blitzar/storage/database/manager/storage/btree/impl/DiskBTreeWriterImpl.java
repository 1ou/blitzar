package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskBTreeWriter;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableBTreeMetadata;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.DiskWriterIoImpl;

import java.io.File;
import java.io.IOException;

public class DiskBTreeWriterImpl implements DiskBTreeWriter {
    /**
     * Disk writer
     */
    private final DiskWriter diskWriter;

    /**
     * Table metadata
     */
    private final TableBTreeMetadata tableBTreeMetadata;

    public DiskBTreeWriterImpl(final File file,
                               final TableBTreeMetadata tableBTreeMetadata) throws IOException {
        this.diskWriter = new DiskWriterIoImpl(file);
        this.tableBTreeMetadata = tableBTreeMetadata;
    }

    @Override
    public void write(int pos, TreeNode node) throws IOException {
        if (!node.leaf) {
            int estimatedSize = tableBTreeMetadata.entriesInNonLeafNodeNumber();
            checkNodeSize(node.q, estimatedSize);

            int currPos = pos;
            diskWriter.write(currPos, new byte[]{0});
            currPos++;
            diskWriter.write(currPos, BytesManipulator.intToBytes(node.q));

            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                int posOfIndex = pos + tableBTreeMetadata.reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * tableBTreeMetadata.primaryIndexNonVariableRecordSize();

                diskWriter.write(currPos, BytesManipulator.intToBytes(posOfIndex));
                diskWriter.write(posOfIndex, BytesManipulator.intToBytes(tableBTreeMetadata.primaryIndexSize()));
                diskWriter.write(posOfIndex + Integer.BYTES, BytesManipulator.intToBytes(node.p[i]));
                byte[] indexValue = node.keys[i].field().value();
                diskWriter.write(posOfIndex + 2 * Integer.BYTES, indexValue);

                if (i == node.q - 1) {
                    diskWriter.write(
                            posOfIndex + 2 * Integer.BYTES + indexValue.length,
                            BytesManipulator.intToBytes(node.p[node.q]));
                }
            }
        } else {
            int estimatedSize = tableBTreeMetadata.entriesInLeafNodeNumber();
            checkNodeSize(node.q, estimatedSize);

            int currPos = pos;
            diskWriter.write(currPos, new byte[]{1});
            currPos++;
            diskWriter.write(currPos, BytesManipulator.intToBytes(node.q));

            for (int i = 0; i < node.q; ++i) {
                currPos += Integer.BYTES;
                int posOfIndex = pos + tableBTreeMetadata.reservedSpaceInNode()
                        + estimatedSize * Integer.BYTES + i * tableBTreeMetadata.dataNonVariableRecordSize();

                diskWriter.write(currPos, BytesManipulator.intToBytes(posOfIndex));
                diskWriter.write(posOfIndex, BytesManipulator.intToBytes(tableBTreeMetadata.primaryIndexSize()));
                diskWriter.write(posOfIndex + Integer.BYTES, BytesManipulator.intToBytes(tableBTreeMetadata.dataSize()));

                diskWriter.write(posOfIndex + 2 * Integer.BYTES, node.keys[i].field().value());
                diskWriter.write(posOfIndex + 2 * Integer.BYTES + tableBTreeMetadata.primaryIndexSize(), node.values[i]);

                if (i == node.q - 1) {
                    diskWriter.write(posOfIndex + 2 * Integer.BYTES + tableBTreeMetadata.primaryIndexSize() + tableBTreeMetadata.dataSize(),
                            BytesManipulator.intToBytes(node.nextPos));
                }
            }
        }
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
