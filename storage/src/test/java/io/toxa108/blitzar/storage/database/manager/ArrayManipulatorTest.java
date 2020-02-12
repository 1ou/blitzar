package io.toxa108.blitzar.storage.database.manager;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ArrayManipulatorTest {
    @Test
    public void insert_in_array_test() {
        ArrayManipulator arrayManipulator = new ArrayManipulator();
        Integer[] arr = new Integer[10];

        for (int i = 0; i < 9; ++i) arr[i] = i;

        arrayManipulator.insertInArray(arr, 66, 4);
        assertArrayEquals(new Integer[]{0, 1, 2, 3, 66, 4, 5, 6, 7, 8}, arr);
    }

    @Test
    public void copy_array_test() {
        ArrayManipulator arrayManipulator = new ArrayManipulator();

        Integer[] arr = new Integer[10];
        for (int i = 0; i < 10; ++i) arr[i] = i;

        Integer[] res = new Integer[4];
        arrayManipulator.copyArray(arr, res, 4, 4);
        assertArrayEquals(new Integer[]{4, 5, 6, 7}, res);
    }

}