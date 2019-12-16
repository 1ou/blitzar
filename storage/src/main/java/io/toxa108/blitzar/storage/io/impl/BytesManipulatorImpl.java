package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.BytesManipulator;

public class BytesManipulatorImpl implements BytesManipulator {

    @Override
    public byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    @Override
    public int bytesToInt(byte[] bytes) {
        return (bytes[0] << 24) & 0xff000000 |
                (bytes[1] << 16) & 0x00ff0000 |
                (bytes[2] << 8) & 0x0000ff00 |
                (bytes[3] << 0) & 0x000000ff;
    }
}
