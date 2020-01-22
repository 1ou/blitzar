package io.toxa108.blitzar.storage.io.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BytesManipulatorTest {
    @Test
    public void int_to_bytes_and_back_when_success() {
        int i = 1000001;
        byte[] bytes = BytesManipulator.intToBytes(i);
        int k = BytesManipulator.bytesToInt(bytes);
        assertEquals(i, k);
    }

    @Test
    public void short_to_bytes_and_back_when_success() {
        short i = (short) 104;
        byte[] bytes = BytesManipulator.shortToBytes(i);
        int k = BytesManipulator.bytesToShort(bytes);
        assertEquals(i, k);
    }


}