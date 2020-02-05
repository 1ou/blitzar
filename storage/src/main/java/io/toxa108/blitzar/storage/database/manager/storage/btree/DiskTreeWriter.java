package io.toxa108.blitzar.storage.database.manager.storage.btree;

import io.toxa108.blitzar.storage.database.manager.storage.btree.impl.TreeNode;

import java.io.IOException;

public interface DiskTreeWriter {
    /**
     * Write tree node to the disk
     *
     * @param pos  file seek
     * @param node node
     * @throws IOException disk io exception
     */
    void write(int pos, TreeNode node) throws IOException;

    /**
     * Update metadata of table
     *
     * @param numberOfBlocks number of blocks
     * @throws IOException disk io exception
     */
    void updateMetadata(int numberOfBlocks) throws IOException;
}
