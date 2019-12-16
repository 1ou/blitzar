package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.DiskPage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DiskPageImpl implements DiskPage {
    private final int size;

    @Override
    public int size() {
        return size;
    }
}
