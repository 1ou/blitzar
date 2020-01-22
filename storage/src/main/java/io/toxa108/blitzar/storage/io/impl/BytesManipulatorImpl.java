package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.io.BytesManipulator;

/**
 * It is the place for static methods but let's try interface...
 */
public class BytesManipulatorImpl implements BytesManipulator {

    @Override
    public byte[] longToBytes(@NotNull final long number) {
        return new byte[]{
                (byte) number,
                (byte) (number >> 8),
                (byte) (number >> 16),
                (byte) (number >> 24),
                (byte) (number >> 32),
                (byte) (number >> 40),
                (byte) (number >> 48),
                (byte) (number >> 56)};
    }

    @Override
    public byte[] intToBytes(@NotNull final int number) {
        return new byte[]{
                (byte) (number >>> 24),
                (byte) (number >>> 16),
                (byte) (number >>> 8),
                (byte) number};
    }

    @Override
    public byte[] shortToBytes(@NotNull final short number) {
        return new byte[]{
                (byte) (number & 0x00FF),
                (byte) ((number & 0xFF00) >> 8)};
    }

    @Override
    public short bytesToShort(@NotNull final byte[] bytes) {
        return (short) (((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF));
    }

    @Override
    public int bytesToInt(@NotNull final byte[] bytes) {
        return (bytes[0] << 24) & 0xff000000 |
                (bytes[1] << 16) & 0x00ff0000 |
                (bytes[2] << 8) & 0x0000ff00 |
                (bytes[3] << 0) & 0x000000ff;
    }

    @Override
    public long bytesToLong(@NotNull final byte[] bytes) {
        return ((long) bytes[7] << 56)
                | ((long) bytes[6] & 0xff) << 48
                | ((long) bytes[5] & 0xff) << 40
                | ((long) bytes[4] & 0xff) << 32
                | ((long) bytes[3] & 0xff) << 24
                | ((long) bytes[2] & 0xff) << 16
                | ((long) bytes[1] & 0xff) << 8
                | ((long) bytes[0] & 0xff);
    }
}
