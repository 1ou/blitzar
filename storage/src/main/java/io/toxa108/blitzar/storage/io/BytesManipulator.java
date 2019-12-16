package io.toxa108.blitzar.storage.io;

public interface BytesManipulator {
    /**
     * Translate int to bytes array
     *
     * @param number int number
     * @return bytes array
     */
    byte[] intToBytes(int number);

    /**
     * Translate bytes to int
     *
     * @param bytes bytes
     * @return int number
     */
    int bytesToInt(byte[] bytes);
}
