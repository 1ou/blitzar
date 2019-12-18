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
     * Translate short to bytes array
     *
     * @param number short number
     * @return bytes array
     */
    byte[] shortToBytes(short number);

    /**
     * Translate bytes to short
     *
     * @param bytes bytes
     * @return short number
     */
    short bytesToShort(byte[] bytes);

    /**
     * Translate bytes to int
     *
     * @param bytes bytes
     * @return int number
     */
    int bytesToInt(byte[] bytes);
}
