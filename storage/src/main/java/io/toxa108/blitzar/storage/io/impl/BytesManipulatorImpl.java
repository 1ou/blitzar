package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.BytesManipulator;

public class BytesManipulatorImpl implements BytesManipulator {

    @Override
    public byte[] intToBytes(int number) {
        return new byte[]{
                (byte) (number >>> 24),
                (byte) (number >>> 16),
                (byte) (number >>> 8),
                (byte) number};
    }

    @Override
    public byte[] shortToBytes(short number) {
        return new byte[]{
                (byte) (number & 0x00FF),
                (byte) ((number & 0xFF00) >> 8)};
    }

    @Override
    public short bytesToShort(byte[] bytes) {
        return (short) (((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF));
    }

    @Override
    public int bytesToInt(byte[] bytes) {
        return (bytes[0] << 24) & 0xff000000 |
                (bytes[1] << 16) & 0x00ff0000 |
                (bytes[2] << 8) & 0x0000ff00 |
                (bytes[3] << 0) & 0x000000ff;
    }
}
