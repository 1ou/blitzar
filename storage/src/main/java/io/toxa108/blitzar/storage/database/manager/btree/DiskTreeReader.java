package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.manager.btree.impl.TreeNode;

import java.io.IOException;

public interface DiskTreeReader {
    /**
     * Read node from disk
     *
     * @param pos pos in file
     * @return tree node
     * @throws IOException in case issue with reading
     */
    TreeNode read(final int pos) throws IOException;
}
