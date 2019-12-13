package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskBlock;
import io.toxa108.blitzar.storage.io.DiskContext;

/**
 * @author toxa
 */
public class DiskContextImpl implements DiskContext {
    private final DiskBlock diskBlock;

    public DiskContextImpl(DiskBlock diskBlock) {
        this.diskBlock = diskBlock;
    }

    @Override
    public int blockSize() {
        return diskBlock.size();
    }
}
