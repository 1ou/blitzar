package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskBlock;
import lombok.AllArgsConstructor;

/**
 * @author toxa
 */
@AllArgsConstructor
public class DiskBlockImpl implements DiskBlock {
    private final int size;

    @Override
    public int size() {
        return size;
    }
}
