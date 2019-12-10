package io.toxa108.blitzar.storage.impl;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InMemoryBPlusTreeRepositoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void search_in_middle_when_unique_error() {
        int q = 30;

        InMemoryBPlusTreeRepository<Integer, Long> inMemoryBPlusTreeRepository =
                new InMemoryBPlusTreeRepository<>(q);
        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            keys.add(i);
        }

        int r = inMemoryBPlusTreeRepository.search(keys, 10);
    }

    @Test
    public void search_in_the_middle_when_success() {
        int q = 30;

        InMemoryBPlusTreeRepository<Integer, Long> inMemoryBPlusTreeRepository =
                new InMemoryBPlusTreeRepository<>(q);
        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 20; i+=2) {
            keys.add(i);
        }

        int r = inMemoryBPlusTreeRepository.search(keys, 9);
    }
}