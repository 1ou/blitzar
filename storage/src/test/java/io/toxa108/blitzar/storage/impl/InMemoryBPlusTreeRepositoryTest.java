package io.toxa108.blitzar.storage.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InMemoryBPlusTreeRepositoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void search_in_middle_when_unique_error() {
        int q = 30;

        InMemoryBPlusTreeRepository<Integer, Long> inMemoryBPlusTreeRepository =
                new InMemoryBPlusTreeRepository<>(q, q);
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
                new InMemoryBPlusTreeRepository<>(q, q);
        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 20; i+=2) {
            keys.add(i);
        }

        int r = inMemoryBPlusTreeRepository.search(keys, 9);
    }

    @Test
    public void insert_in_empty_tree_when_success() {
        int q = 5;
        InMemoryBPlusTreeRepository<Integer, Integer> inMemoryBPlusTreeRepository =
                new InMemoryBPlusTreeRepository<>(5, 5);

        List<Integer> keys = Arrays.asList(10, 5, 11, 12, 6);
        for (Integer key : keys) {
            inMemoryBPlusTreeRepository.add(key, 100);
        }
        Collections.sort(keys);

        for (int i = 0; i < keys.size(); ++i) {
            Assert.assertEquals(keys.get(i), inMemoryBPlusTreeRepository.root().keys.get(i));
        }
    }

    @Test
    public void insert_in_empty_tree_two_levels_when_success() {
        InMemoryBPlusTreeRepository<Integer, Integer> inMemoryBPlusTreeRepository =
                new InMemoryBPlusTreeRepository<>(3, 2);

        List<Integer> keys = Arrays.asList(5, 8, 1, 7, 3);
        for (Integer key : keys) {
            inMemoryBPlusTreeRepository.add(key, 100);
        }

        inMemoryBPlusTreeRepository.add(12, 100);
        inMemoryBPlusTreeRepository.add(9, 100);
        inMemoryBPlusTreeRepository.add(6, 100);
        Collections.sort(keys);

        for (int i = 0; i < keys.size(); ++i) {
            Assert.assertEquals(keys.get(i), inMemoryBPlusTreeRepository.root().keys.get(i));
        }
    }

}