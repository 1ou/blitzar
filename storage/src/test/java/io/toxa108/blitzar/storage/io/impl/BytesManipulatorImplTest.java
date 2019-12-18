package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.io.BytesManipulator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BytesManipulatorImplTest {
    @Test
    public void int_to_bytes_and_back_when_success() {
        BytesManipulator bytesManipulator = new BytesManipulatorImpl();
        int i = 1000001;
        byte[] bytes = bytesManipulator.intToBytes(i);
        int k = bytesManipulator.bytesToInt(bytes);
        assertEquals(i, k);
    }

    @Test
    public void short_to_bytes_and_back_when_success() {
        BytesManipulator bytesManipulator = new BytesManipulatorImpl();
        short i = (short) 104;
        byte[] bytes = bytesManipulator.shortToBytes(i);
        int k = bytesManipulator.bytesToShort(bytes);
        assertEquals(i, k);
    }


}