package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeWriter;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.DiskNioWriter;
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

    public BzDiskTreeWriter(final File file,
                            final TableTreeMetadata tableTreeMetadata) throws IOException {
        this.diskWriter = new DiskNioWriter(file);
        this.tableTreeMetadata = tableTreeMetadata;
    }

    @Override
    public void write(final int pos, final TreeNode node) throws IOException {
        log.info("Write to: " + pos);

        diskWriter.write(pos, new BzTreeNodeToBytes(node, pos, tableTreeMetadata).transform());
    }

    @Override
    public void updateMetadata(final int numberOfBlocks) throws IOException {
        diskWriter.write(0, BytesManipulator.intToBytes(numberOfBlocks));
    }
}
