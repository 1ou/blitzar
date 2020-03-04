package io.toxa108.blitzar.storage.threadsafe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Stack;

public class StackTest {
    @Test
    public void test_Ok() {
        final Stack<Integer> positions = new Stack<>();
        positions.push(1);
        positions.push(2);
        positions.push(3);

        Assertions.assertTrue((((Integer) positions.toArray()[0]) == 1));
        Assertions.assertTrue((((Integer) positions.toArray()[1]) == 2));
        Assertions.assertTrue((((Integer) positions.toArray()[2]) == 3));

        Assertions.assertTrue((positions.size() == 3));
    }
}
