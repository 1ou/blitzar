package io.toxa108.blitzar.storage.impl;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * In memory red black tree implementation
 */
public class InMemoryTreeRepository<K, V> implements Repository<K, V> {
    private final TreeMap<K, V> treeMap;

    public InMemoryTreeRepository(Comparator<K> comparator) {
        this.treeMap = new TreeMap<K, V>(comparator);
    }

    @Override
    public V add(K key, V value) {
        return treeMap.put(key, value);
    }

    @Override
    public List<Result<K, V>> all() {
        return treeMap.entrySet()
                .stream()
                .map(e -> new Result<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }
}
